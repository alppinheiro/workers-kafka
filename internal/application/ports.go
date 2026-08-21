package application

import (
	"context"
	"errors"

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
	// Process autoriza/efetua o pagamento e retorna se foi aprovado, o transactionID gerado e um erro temporário.
	Process(ctx context.Context, orderID string) (approved bool, transactionID string, err error)
	// Refund tenta estornar um pagamento previamente efetuado identificado pelo transactionID.
	Refund(ctx context.Context, orderID string, transactionID string) (refunded bool, err error)
}

// InventoryGateway simula a chamada à API externa de estoque.
type InventoryGateway interface {
	Reserve(ctx context.Context, orderID string) (reserved bool, err error)
}

// NotificationGateway simula a chamada à API externa de notificação.
type NotificationGateway interface {
	Notify(ctx context.Context, orderID string) (sent bool, err error)
}

// SagaRepository persiste e recupera o estado corrente de uma saga por order_id.
type SagaRepository interface {
	Save(ctx context.Context, saga domain.Saga) error
	Load(ctx context.Context, orderID string) (domain.Saga, error)
}

// ErrSagaNotFound é retornado por SagaRepository.Load quando não existe saga para o order_id.
var ErrSagaNotFound = errors.New("saga not found")

// Directions possíveis para um registro do journal de eventos (EventLogEntry).
const (
	// DirectionIn marca um evento consumido/entrada de um componente.
	DirectionIn = "IN"
	// DirectionOut marca um evento publicado/saída de um componente.
	DirectionOut = "OUT"
	// DirectionGatewayRequest marca a chamada de saída a um gateway externo.
	DirectionGatewayRequest = "GATEWAY_REQUEST"
	// DirectionGatewayResponse marca a resposta recebida de um gateway externo.
	DirectionGatewayResponse = "GATEWAY_RESPONSE"
)

// EventLogEntry representa uma linha do journal de eventos: a visão de um componente
// sobre um evento do barramento e, quando aplicável, os payloads de request/response
// trocados com gateways externos.
type EventLogEntry struct {
	OrderID         string
	SagaID          string
	EventID         string
	EventType       domain.EventType
	Component       string
	Direction       string
	StatusAnterior  domain.OrderStatus
	StatusAtual     domain.OrderStatus
	Payload         any
	RequestPayload  any
	ResponsePayload any
}

// EventLogRepository persiste o journal de eventos (append-only) para rastreabilidade.
type EventLogRepository interface {
	Append(ctx context.Context, entry EventLogEntry) error
	// Has informa se um evento já foi registrado por um componente (idempotência).
	Has(ctx context.Context, eventID string, component string) (bool, error)
}

// OrderViewRepository persiste o read model de pedidos no banco de leitura.
type OrderViewRepository interface {
	// ApplyEvent atualiza o read model a partir de um evento do barramento.
	ApplyEvent(ctx context.Context, event domain.Event) error
	// MarkProcessed registra o event_id como processado; retorna false se já existia.
	MarkProcessed(ctx context.Context, eventID string) (bool, error)
}
