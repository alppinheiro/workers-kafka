package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"workers-kafka/internal/domain"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
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

// Publish resolve o tópico do evento e o registra na outbox.
func (p *Publisher) Publish(ctx context.Context, event domain.Event) error {
	topic, ok := infrakafka.TopicForEventType(event.EventType)
	if !ok {
		return fmt.Errorf("nenhum tópico configurado para o tipo de evento %s", event.EventType)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("erro ao serializar evento %s: %w", event.EventID, err)
	}

	return p.outbox.Append(ctx, infrapostgres.OutboxEntry{
		EventID: event.EventID,
		Topic:   topic,
		Key:     event.OrderID,
		Payload: payload,
	})
}
