package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	kafkago "github.com/segmentio/kafka-go"

	infrakafka "workers-kafka/internal/infrastructure/kafka"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
)

const (
	pollInterval = time.Second
	batchSize    = 100
)

// main roda o relé da outbox: lê eventos não publicados da tabela outbox, publica no
// Kafka e marca como publicado. É o componente que garante a entrega dos eventos
// decididos pelos orquestrador/workers, mesmo que o processo que os gerou tenha morrido.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("outbox-relay")
	if err != nil {
		log.Fatalf("outbox-relay: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("outbox-relay: %v", err)
	}
	defer pool.Close()

	outbox := infrapostgres.NewOutboxRepository(pool)
	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	log.Println("outbox-relay: aguardando eventos na outbox")
	for {
		select {
		case <-ctx.Done():
			log.Println("outbox-relay: encerrado")
			return
		case <-time.After(pollInterval):
			if err := relayOnce(ctx, outbox, producer); err != nil {
				log.Printf("outbox-relay: erro no ciclo: %v", err)
			}
		}
	}
}

func relayOnce(ctx context.Context, outbox *infrapostgres.OutboxRepository, producer *infrakafka.Producer) error {
	entries, err := outbox.FetchPending(ctx, batchSize)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
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

	if err := producer.PublishBatch(ctx, msgs); err != nil {
		return fmt.Errorf("erro ao publicar lote da outbox: %w", err)
	}
	for _, span := range spans {
		span.End()
	}

	for _, entry := range entries {
		if err := outbox.MarkPublished(ctx, entry.ID); err != nil {
			return fmt.Errorf("erro ao marcar evento %s como publicado: %w", entry.EventID, err)
		}
		log.Printf("outbox-relay: publicado event_id=%s topic=%s key=%s", entry.EventID, entry.Topic, entry.Key)
	}
	return nil
}
