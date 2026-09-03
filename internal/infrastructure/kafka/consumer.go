package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
	"workers-kafka/internal/infrastructure/metrics"
)

const consumerRetryDelay = 2 * time.Second

// ConsumerConfig descreve os tópicos que um Consumer deve acompanhar dentro de um consumer group.
type ConsumerConfig struct {
	Brokers []string
	GroupID string
	// ServiceName identifica o serviço nos spans (OpenTelemetry).
	ServiceName string
	// Workers define quantas goroutines de consumo rodam no mesmo consumer group.
	// O Kafka distribui as partições entre elas; 1 (default) preserva o comportamento
	// sequencial original. Em produção, Workers >= partições não traz ganho adicional.
	Workers int
	// CommitBatch define quantas mensagens acumular antes de commitar os offsets em lote
	// (reduz os round-trips ao broker). Default: KAFKA_COMMIT_BATCH (50).
	CommitBatch int
	// CommitInterval é o intervalo máximo entre commits em lote.
	// Default: KAFKA_COMMIT_INTERVAL (200ms).
	CommitInterval time.Duration
	// Topic é usado quando o consumer acompanha um único tópico.
	Topic string
	// Topics permite acompanhar múltiplos tópicos com um único reader, evitando goroutines adicionais.
	Topics []string
	// DLQWriter, quando definido, recebe mensagens com erro definitivo (application.ErrNonRetryable),
	// que são movidas para o tópico DLQ correspondente e commitadas.
	DLQWriter *kafkago.Writer
}

// Consumer lê eventos de Kafka e os repassa, já desserializados, para um application.EventHandler.
type Consumer struct {
	cfg            ConsumerConfig
	workers        int
	commitBatch    int
	commitInterval time.Duration
	dlq            *kafkago.Writer
	serviceName    string

	mu      sync.Mutex
	readers []*kafkago.Reader
}

// NewConsumer cria um consumer a partir da configuração informada.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	workers := cfg.Workers
	if workers < 1 {
		workers = 1
	}
	commitBatch := cfg.CommitBatch
	if commitBatch < 1 {
		commitBatch = CommitBatchFromEnv()
	}
	commitInterval := cfg.CommitInterval
	if commitInterval < 10*time.Millisecond {
		commitInterval = CommitIntervalFromEnv()
	}
	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "consumer"
	}
	return &Consumer{
		cfg:            cfg,
		workers:        workers,
		commitBatch:    commitBatch,
		commitInterval: commitInterval,
		dlq:            cfg.DLQWriter,
		serviceName:    serviceName,
	}
}

// Consume lança `workers` goroutines, cada uma com seu próprio Reader no mesmo consumer
// group: o Kafka distribui as partições entre elas (rebalanceamento automático), e a
// ordem é preservada por partição (logo, por order_id). Retorna o primeiro erro.
func (c *Consumer) Consume(ctx context.Context, handler application.EventHandler) error {
	eg, ectx := errgroup.WithContext(ctx)
	for i := 0; i < c.workers; i++ {
		eg.Go(func() error {
			return c.consumeWorker(ectx, handler)
		})
	}
	return eg.Wait()
}

// consumeWorker roda o loop de consumo com um Reader próprio do consumer group.
// Os offsets são commitados em lote (por contagem ou intervalo) para reduzir os
// round-trips ao broker; um commit que falha NUNCA derruba o serviço — a mensagem
// não commitada é reprocessada na sequência (idempotência por event_id cobre).
func (c *Consumer) consumeWorker(ctx context.Context, handler application.EventHandler) error {
	newReader := func() *kafkago.Reader {
		r := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     c.cfg.Brokers,
			GroupID:     c.cfg.GroupID,
			Topic:       c.cfg.Topic,
			GroupTopics: c.cfg.Topics,
		})
		c.mu.Lock()
		c.readers = append(c.readers, r)
		c.mu.Unlock()
		return r
	}
	reader := newReader()
	defer func() { _ = reader.Close() }()

	// Watchdog anti-stall: o kafka-go pode parar de fazer fetch ("reader travado") sem
	// erro aparente, mantendo o membro vivo no grupo mas sem consumir. Ao detectar o
	// stall, o watchdog CANCELA o contexto do FetchMessage (que está bloqueado), e o
	// loop reconecta o reader (self-healing).
	var fetchCtx context.Context
	var fetchCancel context.CancelFunc
	startFetch := func() {
		fetchCtx, fetchCancel = context.WithCancel(ctx)
	}
	startFetch()

	stallStop := make(chan struct{})
	startWatchdog := func() {
		go c.watchdogStall(reader, fetchCancel, stallStop)
	}
	startWatchdog()
	defer close(stallStop)

	batcher := newCommitBatcher(c.commitBatch, c.commitInterval)
	var pendingCommits []kafkago.Message

	// flush commita as mensagens acumuladas em um único round-trip ao broker.
	flush := func() error {
		if len(pendingCommits) == 0 {
			return nil
		}
		err := reader.CommitMessages(ctx, pendingCommits...)
		batcher.reset(time.Now())
		pendingCommits = pendingCommits[:0]
		return err
	}

	for {
		msg, err := reader.FetchMessage(fetchCtx)
		if err != nil {
			if fetchCtx.Err() != nil {
				// Stall detectado pelo watchdog: reconecta o reader (self-healing).
				_ = flush()
				slog.Info("reconectando reader", "component", "consumer", "phase", "reconnect", "service", c.serviceName)
				metrics.RecordConsumerReconnect(c.serviceName)
				if closeErr := reader.Close(); closeErr != nil {
					slog.Error("erro ao fechar reader", "component", "consumer", "phase", "reconnect-close", "error", closeErr)
				}
				close(stallStop)
				reader = newReader()
				stallStop = make(chan struct{})
				startFetch()
				startWatchdog()
				continue
			}
			if shouldRetryFetch(err) {
				slog.Warn("erro de fetch transitório, retentando",
					"component", "consumer", "phase", "retry-fetch", "delay", consumerRetryDelay, "error", err)
				if waitErr := waitForRetry(ctx, consumerRetryDelay); waitErr != nil {
					_ = flush()
					return waitErr
				}
				continue
			}
			_ = flush()
			return fmt.Errorf("erro ao ler mensagem: %w", err)
		}

		var event domain.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			moved, moveErr := c.moveToDLQ(ctx, reader, msg, fmt.Errorf("%w: erro ao desserializar evento: %v", application.ErrNonRetryable, err))
			if moveErr != nil {
				return moveErr
			}
			if moved {
				continue
			}
			return fmt.Errorf("erro ao desserializar evento: %w", err)
		}

		// Validação de contrato: evento com schema_version desconhecido (≠ CurrentSchemaVersion)
		// é movido para a DLQ em vez de ser processado silenciosamente com payload incompleto —
		// evita falhas estranhas quando um producer novo publica um contrato incompatível
		// (o json.Unmarshal preencheria campos faltantes com zero e ignoraria campos novos).
		if err := validateSchemaVersion(event); err != nil {
			moved, moveErr := c.moveToDLQ(ctx, reader, msg, fmt.Errorf("%w: %v", application.ErrNonRetryable, err))
			if moveErr != nil {
				return moveErr
			}
			if moved {
				continue
			}
			return err
		}

		// Propaga o trace (W3C traceparent) dos headers e abre um span por evento.
		handlerCtx := extractTraceContext(ctx, msg.Headers)
		handlerCtx, span := c.tracer().Start(handlerCtx, "consume "+string(event.EventType),
			trace.WithAttributes(
				attribute.String("order_id", event.OrderID),
				attribute.String("event_id", event.EventID),
			))

		metrics.RecordReceived(c.serviceName, string(event.EventType))
		start := time.Now()
		err = handler(handlerCtx, event)
		span.End()

		if err != nil {
			metrics.RecordError(c.serviceName, string(event.EventType))
			moved, moveErr := c.moveToDLQ(handlerCtx, reader, msg, fmt.Errorf("erro ao processar evento %s: %w", event.EventID, err))
			if moveErr != nil {
				return moveErr
			}
			if moved {
				continue
			}
			return err
		}
		metrics.RecordProcessed(c.serviceName, string(event.EventType), time.Since(start))

		// Acumula o offset e commita em lote (por contagem ou intervalo).
		pendingCommits = append(pendingCommits, msg)
		batcher.add()
		if batcher.shouldFlush(time.Now()) {
			if err := flush(); err != nil {
				// Não-fatal: a mensagem não commitada será reprocessada (idempotência).
				if shouldRetryCommit(err) {
					slog.Warn("commit falhou, será re-tentado", "component", "consumer", "phase", "commit-retry", "error", err)
				} else {
					slog.Error("erro no commit de offsets", "component", "consumer", "phase", "commit-error", "error", err)
				}
			}
		}
	}
}

// Close fecha todos os readers criados pelos workers.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, r := range c.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// moveToDLQ move uma mensagem com erro definitivo para a DLQ do tópico de origem e a
// commita, retornando true. Se não houver DLQ configurada ou o erro não for definitivo
// (transitório), retorna false e a mensagem permanece pendente para retry.
func (c *Consumer) moveToDLQ(ctx context.Context, reader *kafkago.Reader, msg kafkago.Message, err error) (bool, error) {
	if c.dlq == nil || !errors.Is(err, application.ErrNonRetryable) {
		return false, nil
	}

	dlqTopic := DLQTopicFor(msg.Topic)
	if writeErr := c.dlq.WriteMessages(ctx, kafkago.Message{Topic: dlqTopic, Key: msg.Key, Value: msg.Value}); writeErr != nil {
		return false, fmt.Errorf("erro ao mover mensagem para a DLQ %s: %w", dlqTopic, writeErr)
	}

	slog.Warn("mensagem movida para DLQ",
		"component", "consumer", "phase", "dlq", "topic", msg.Topic, "dlq_topic", dlqTopic, "offset", msg.Offset, "error", err)
	metrics.RecordDLQ(msg.Topic)

	// O commit da mensagem movida pode falhar transitoriamente (ex.: tópico recriado).
	// Não é fatal: a mensagem será reprocessada (idempotência) e re-movida.
	if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
		slog.Error("erro ao commitar mensagem movida para DLQ", "component", "consumer", "phase", "dlq-commit-error", "topic", msg.Topic, "error", commitErr)
	}
	return true, nil
}

// validateSchemaVersion garante que o evento use o contrato atual (schema_version == 1).
// Retorna erro definitivo quando o schema é desconhecido — o consumer deve mover o evento
// para a DLQ em vez de processar silenciosamente um payload que pode estar incompleto.
func validateSchemaVersion(event domain.Event) error {
	if event.SchemaVersion != domain.CurrentSchemaVersion {
		return fmt.Errorf("schema_version %d não suportado (esperado %d) para o evento %s do pedido %s",
			event.SchemaVersion, domain.CurrentSchemaVersion, event.EventID, event.OrderID)
	}
	return nil
}

// extractTraceContext recupera o contexto de trace (W3C traceparent) dos headers do Kafka.
func extractTraceContext(ctx context.Context, headers []kafkago.Header) context.Context {
	carrier := make(propagation.MapCarrier)
	for _, h := range headers {
		carrier[h.Key] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// tracer retorna o tracer nomeado pelo serviço (OpenTelemetry).
func (c *Consumer) tracer() trace.Tracer {
	return otel.Tracer(c.serviceName)
}

// shouldRetryFetch indica se um erro de leitura/coordenação do Kafka é transitório e
// deve ser re-tentado com backoff em vez de derrubar o serviço.
func shouldRetryFetch(err error) bool {
	return errors.Is(err, kafkago.UnknownTopicOrPartition) ||
		errors.Is(err, kafkago.LeaderNotAvailable) ||
		errors.Is(err, kafkago.NotLeaderForPartition) ||
		errors.Is(err, kafkago.BrokerNotAvailable) ||
		errors.Is(err, kafkago.GroupCoordinatorNotAvailable) ||
		errors.Is(err, kafkago.NotCoordinatorForGroup) ||
		errors.Is(err, kafkago.GroupLoadInProgress) ||
		errors.Is(err, kafkago.RebalanceInProgress) ||
		errors.Is(err, kafkago.UnknownMemberId)
}

// shouldRetryCommit classifica erros de commit como transitórios (mesmos erros de
// coordenação do fetch). O commit falho NUNCA derruba o serviço: a mensagem não
// commitada é reprocessada e a idempotência por event_id garante a consistência.
func shouldRetryCommit(err error) bool {
	return shouldRetryFetch(err)
}

const (
	// stallCheckInterval é a frequência de checagem do watchdog anti-stall.
	stallCheckInterval = 15 * time.Second
	// stallTimeout é o tempo sem NENHUM fetch do Kafka que indica reader travado.
	stallTimeout = 45 * time.Second
)

// watchdogStall monitora o reader e, ao detectar que ele parou de fazer fetch
// (kafka-go pode "travar" sem erro aparente, mantendo o membro vivo no grupo mas sem
// consumir), CANCELA o contexto do FetchMessage — desbloqueando o loop principal, que
// reconecta o reader (self-healing).
func (c *Consumer) watchdogStall(reader *kafkago.Reader, cancel context.CancelFunc, stop <-chan struct{}) {
	ticker := time.NewTicker(stallCheckInterval)
	defer ticker.Stop()

	var lastFetches int64 = -1
	lastProgress := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			stats := reader.Stats()
			now := time.Now()
			metrics.SetConsumerLastProgress(c.serviceName, now.Sub(lastProgress).Seconds())
			if stats.Fetches > lastFetches {
				lastFetches = stats.Fetches
				lastProgress = now
				continue
			}
			if stallDetected(stats.Fetches, lastFetches, now.Sub(lastProgress), stallTimeout) {
				slog.Warn("reader travado detectado",
					"component", "consumer", "phase", "stall-detected", "service", c.serviceName,
					"fetches", stats.Fetches, "lag", stats.Lag, "sem_progresso", now.Sub(lastProgress).Round(time.Second))
				cancel()
				lastProgress = now
			}
		}
	}
}

// stallDetected indica que o contador de fetches não avançou (current <= last) por pelo
// menos timeout — ou seja, o reader não está mais buscando mensagens do broker.
func stallDetected(current, last int64, elapsed, timeout time.Duration) bool {
	return current <= last && elapsed >= timeout
}

// commitBatcher decide quando commitar offsets em lote: por contagem acumulada ou por
// intervalo desde o último commit. Extraído para teste unitário isolado.
type commitBatcher struct {
	batch    int
	interval time.Duration
	pending  int
	last     time.Time
}

func newCommitBatcher(batch int, interval time.Duration) *commitBatcher {
	return &commitBatcher{batch: batch, interval: interval, last: time.Now()}
}

func (b *commitBatcher) add() { b.pending++ }

func (b *commitBatcher) shouldFlush(now time.Time) bool {
	return b.pending >= b.batch || now.Sub(b.last) >= b.interval
}

func (b *commitBatcher) reset(now time.Time) {
	b.pending = 0
	b.last = now
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
