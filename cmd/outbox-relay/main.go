package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	kafkago "github.com/segmentio/kafka-go"

	"workers-kafka/internal/infrastructure/health"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
)

const (
	defaultBatchSize = 500
	defaultBackoff   = 250 * time.Millisecond
	claimTimeout     = 60 * time.Second
	purgeInterval    = time.Hour
	purgeAfter       = 7 * 24 * time.Hour
	countInterval    = 10 * time.Second
	// relayStallTimeout é o tempo sem nenhum ciclo concluído que torna o /healthz
	// "não saudável" (detecta stall do loop principal do relay).
	relayStallTimeout = 30 * time.Second
)

// main roda o relé da outbox: lê eventos não publicados da tabela outbox, publica no
// Kafka e marca como publicado. É o componente que garante a entrega dos eventos
// decididos pelos orquestrador/workers, mesmo que o processo que os gerou tenha morrido.
//
// O loop é contínuo: quando há backlog, o próximo lote é processado imediatamente
// (sem o teto de "1 ciclo por segundo" do timer fixo original); com a outbox vazia,
// aguarda um backoff curto. Configurável via ambiente:
//
//	OUTBOX_BATCH_SIZE    (default 500)
//	OUTBOX_POLL_INTERVAL (default 250ms)
func main() {
	logging.Setup("outbox-relay")
	brokers := infrakafka.BrokersFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("outbox-relay")
	if err != nil {
		slog.Error("falha ao inicializar telemetria", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		slog.Error("falha ao conectar no postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// lastActivity registra a conclusão de cada ciclo do loop principal; o /healthz
	// do relay retorna 503 se o loop ficar sem atividade por mais de relayStallTimeout.
	var lastActivity atomic.Int64

	metrics.ServeWithChecks(":9106",
		health.Postgres(pool),
		health.Kafka(brokers),
		health.LastActivity(&lastActivity, relayStallTimeout),
	)

	batchSize := envInt("OUTBOX_BATCH_SIZE", defaultBatchSize)
	if batchSize < 1 {
		batchSize = defaultBatchSize
	}
	pollBackoff := envDuration("OUTBOX_POLL_INTERVAL", defaultBackoff)
	if pollBackoff < 10*time.Millisecond {
		pollBackoff = defaultBackoff
	}

	outbox := infrapostgres.NewOutboxRepository(pool)
	producer := infrakafka.NewProducer(brokers, "outbox-relay")
	defer func() { _ = producer.Close() }()

	// Métrica de backlog atualizada de forma esparsa (evita um SELECT count por ciclo).
	go func() {
		ticker := time.NewTicker(countInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if pending, err := outbox.CountPending(ctx); err == nil {
					metrics.SetOutboxPending(pending)
				}
			}
		}
	}()

	// Retenção: purga eventos publicados há mais de purgeAfter (a outbox cresceria sem limite).
	go func() {
		ticker := time.NewTicker(purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				removed, err := outbox.PurgePublished(ctx, purgeAfter)
				if err != nil {
					slog.Error("erro na purga da outbox", "error", err)
					continue
				}
				if removed > 0 {
					slog.Info("purga da outbox", "removida", removed, "older_than", purgeAfter)
				}
			}
		}
	}()

	slog.Info("outbox-relay iniciado", "batch_size", batchSize, "poll_backoff", pollBackoff)
	for {
		n, err := relayOnce(ctx, outbox, producer, batchSize)
		lastActivity.Store(time.Now().UnixNano())
		if err != nil {
			slog.Error("erro no ciclo do relay", "error", err)
			if !sleepCtx(ctx, pollBackoff) {
				return
			}
			continue
		}
		// Lote cheio → processa o próximo imediatamente (alta vazão).
		// Lote vazio → backoff curto (sem busy-loop).
		if n == 0 {
			if !sleepCtx(ctx, pollBackoff) {
				return
			}
		}
	}
}

// relayOnce executa um ciclo: claim → publish → mark em lote. Retorna o nº de eventos
// publicados (0 quando a outbox está vazia, permitindo ao chamador aplicar backoff).
func relayOnce(ctx context.Context, outbox *infrapostgres.OutboxRepository, producer *infrakafka.Producer, batchSize int) (int, error) {
	entries, err := outbox.ClaimPending(ctx, batchSize, claimTimeout)
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, nil
	}

	// Monta o lote, reconstruindo o trace do produtor original (traceparent salvo na
	// outbox) para manter a cadeia distribuída em cada span outbox.publish.
	msgs := make([]kafkago.Message, 0, len(entries))
	spans := make([]trace.Span, 0, len(entries))
	for _, entry := range entries {
		publishCtx := ctx
		if entry.Traceparent != "" {
			carrier := propagation.MapCarrier{"traceparent": entry.Traceparent}
			publishCtx = otel.GetTextMapPropagator().Extract(ctx, carrier)
		}
		_, span := otel.Tracer("outbox-relay").Start(publishCtx, "outbox.publish",
			trace.WithAttributes(
				attribute.String("event_id", entry.EventID),
				attribute.String("order_id", entry.Key),
				attribute.String("topic", entry.Topic),
			))
		spans = append(spans, span)

		msgs = append(msgs, kafkago.Message{
			Topic:   entry.Topic,
			Key:     []byte(entry.Key),
			Value:   entry.Payload,
			Headers: infrakafka.TraceHeadersFrom(publishCtx),
		})
	}

	started := time.Now()
	if err := producer.PublishBatch(ctx, msgs); err != nil {
		endSpans(spans)
		return 0, fmt.Errorf("erro ao publicar lote da outbox: %w", err)
	}
	endSpans(spans)

	// Marca o lote como publicado em 1 round-trip (não 1 UPDATE por evento).
	ids := make([]int64, len(entries))
	for i, entry := range entries {
		ids[i] = entry.ID
	}
	if err := outbox.MarkPublishedBatch(ctx, ids); err != nil {
		return 0, fmt.Errorf("erro ao marcar lote da outbox como publicado: %w", err)
	}
	for range entries {
		metrics.RecordOutboxPublished()
	}

	slog.Info("ciclo do relay concluído", "publicados", len(entries), "duracao", time.Since(started))
	return len(entries), nil
}

func endSpans(spans []trace.Span) {
	for _, span := range spans {
		span.End()
	}
}

// sleepCtx dorme por d ou retorna false quando o contexto é cancelado.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// envInt lê um inteiro do ambiente com default.
func envInt(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("valor de ambiente inválido, usando default", "name", name, "raw", raw, "default", def)
		return def
	}
	return n
}

// envDuration lê uma duração do ambiente com default.
func envDuration(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		slog.Warn("valor de ambiente inválido, usando default", "name", name, "raw", raw, "default", def)
		return def
	}
	return d
}
