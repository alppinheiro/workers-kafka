package worker

import (
	"context"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// Coordinator dispara o evento inicial do pedido, sem executar regra de negócio das etapas seguintes.
type Coordinator struct {
	publisher application.EventPublisher
}

// NewCoordinator monta o coordenador a partir do publisher usado para anunciar a criação do pedido.
func NewCoordinator(publisher application.EventPublisher) *Coordinator {
	return &Coordinator{publisher: publisher}
}

// CreateOrder publica o evento de criação do pedido com status inicial PENDING.
func (c *Coordinator) CreateOrder(ctx context.Context, orderID string) error {
	event := domain.Event{
		EventID:       domain.NewEventID(),
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   domain.StatusPending,
		EventType:     domain.EventOrderCreated,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}

	return c.publisher.Publish(ctx, event)
}
