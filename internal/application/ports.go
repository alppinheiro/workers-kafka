package application

import (
	"context"

	"workers-kafka/internal/domain"
)

// EventPublisher publica um evento de domínio, decidindo o tópico de destino a partir do seu EventType.
type EventPublisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

// EventHandler processa um único evento consumido de um tópico.
type EventHandler func(ctx context.Context, event domain.Event) error

// EventConsumer consome eventos de um tópico já configurado e os repassa para um EventHandler.
type EventConsumer interface {
	Consume(ctx context.Context, handler EventHandler) error
}

// PaymentGateway simula a chamada à API externa de pagamento.
type PaymentGateway interface {
	Process(ctx context.Context, orderID string) (approved bool, err error)
}

// InventoryGateway simula a chamada à API externa de estoque.
type InventoryGateway interface {
	Reserve(ctx context.Context, orderID string) (reserved bool, err error)
}

// NotificationGateway simula a chamada à API externa de notificação.
type NotificationGateway interface {
	Notify(ctx context.Context, orderID string) (sent bool, err error)
}
