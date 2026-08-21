package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	cfg         ConsumerConfig
	workers     int
	dlq         *kafkago.Writer
	serviceName string

	mu      sync.Mutex
	readers []*kafkago.Reader
}

// NewConsumer cria um consumer a partir da configuração informada.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	workers := cfg.Workers
	if workers < 1 {
		workers = 1
	}
	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "consumer"
	}
	return &Consumer{
		cfg:         cfg,
		workers:     workers,
		dlq:         cfg.DLQWriter,
		serviceName: serviceName,
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
func (c *Consumer) consumeWorker(ctx context.Context, handler application.EventHandler) error {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     c.cfg.Brokers,
		GroupID:     c.cfg.GroupID,
		Topic:       c.cfg.Topic,
		GroupTopics: c.cfg.Topics,
	})
	c.mu.Lock()
	c.readers = append(c.readers, reader)
	c.mu.Unlock()
	defer func() { _ = reader.Close() }()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if shouldRetryFetch(err) {
				log.Printf("component=consumer phase=retry-fetch delay=%s error=%v", consumerRetryDelay, err)
				if waitErr := waitForRetry(ctx, consumerRetryDelay); waitErr != nil {
					return waitErr
				}
				continue
			}
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

		if err := reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("erro ao confirmar mensagem: %w", err)
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

	log.Printf("component=consumer phase=dlq topic=%s dlq_topic=%s offset=%d error=%v", msg.Topic, dlqTopic, msg.Offset, err)
	metrics.RecordDLQ(msg.Topic)

	if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
		return false, fmt.Errorf("erro ao confirmar mensagem movida para a DLQ: %w", commitErr)
	}
	return true, nil
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

func shouldRetryFetch(err error) bool {
	return err == kafkago.GroupCoordinatorNotAvailable ||
		err == kafkago.NotCoordinatorForGroup ||
		err == kafkago.GroupLoadInProgress
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
