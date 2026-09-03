package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"workers-kafka/internal/domain"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	metrics "workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
)

// Publisher implementa application.EventPublisher gravando o evento na tabela outbox
// do banco de escrita (Outbox Pattern). O serviço outbox-relay publica de fato no Kafka.
type Publisher struct {
	outbox *infrapostgres.OutboxRepository
}

// NewPublisher cria um publisher de outbox sobre o repositório informado.
func NewPublisher(outbox *infrapostgres.OutboxRepository) *Publisher {
	return &Publisher{outbox: outbox}
}

// Publish resolve o tópico do evento e o registra na outbox, preservando o trace
// corrente (W3C traceparent) para que o outbox-relay continue a cadeia distribuída.
func (p *Publisher) Publish(ctx context.Context, event domain.Event) error {
	topic, ok := infrakafka.TopicForEventType(event.EventType)
	if !ok {
		return fmt.Errorf("nenhum tópico configurado para o tipo de evento %s", event.EventType)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("erro ao serializar evento %s: %w", event.EventID, err)
	}

	if err := p.outbox.Append(ctx, infrapostgres.OutboxEntry{
		EventID:     event.EventID,
		Topic:       topic,
		Key:         event.OrderID,
		Payload:     payload,
		Traceparent: extractTraceparent(ctx),
	}); err != nil {
		return err
	}
	afterAppend(event)
	return nil
}

// afterAppend registra métricas P0 após enfileirar na outbox: eventos gerados (todos)
// e o outcome terminal (COMPLETED/FAILED) quando o evento encerra a saga.
func afterAppend(event domain.Event) {
	metrics.RecordOutboxGenerated()
	switch event.EventType {
	case domain.EventOrderCompleted:
		metrics.RecordTerminal("COMPLETED")
	case domain.EventOrderFailed:
		metrics.RecordTerminal("FAILED")
	}
}

// extractTraceparent serializa o trace corrente (W3C traceparent) para ser propagado
// pelo outbox-relay no momento da publicação no Kafka.
func extractTraceparent(ctx context.Context) string {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}
