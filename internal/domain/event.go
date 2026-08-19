package domain

import "time"

// EventType diferencia comandos, resultados e a criação inicial do pedido dentro de um mesmo tópico.
type EventType string

const (
	EventOrderCreated        EventType = "ORDER_CREATED"
	EventOrderCompleted      EventType = "ORDER_COMPLETED"
	EventOrderFailed         EventType = "ORDER_FAILED"
	EventPaymentCommand      EventType = "PAYMENT_COMMAND"
	EventPaymentResult       EventType = "PAYMENT_RESULT"
	EventInventoryCommand    EventType = "INVENTORY_COMMAND"
	EventInventoryResult     EventType = "INVENTORY_RESULT"
	EventNotificationCommand EventType = "NOTIFICATION_COMMAND"
	EventNotificationResult  EventType = "NOTIFICATION_RESULT"
)

// CurrentSchemaVersion identifica o formato do payload de Event usado pelos producers atuais.
const CurrentSchemaVersion = 1

// Event é o contrato de mensagem trocado entre orquestrador e workers via Kafka.
type Event struct {
	EventID        string            `json:"event_id"`
	OrderID        string            `json:"order_id"`
	SagaID         string            `json:"saga_id,omitempty"`
	StatusAtual    OrderStatus       `json:"status_atual"`
	StatusAnterior OrderStatus       `json:"status_anterior,omitempty"`
	EventType      EventType         `json:"event_type"`
	SchemaVersion  int               `json:"schema_version"`
	CreatedAt      time.Time         `json:"created_at"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}
