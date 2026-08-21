package kafka

import "workers-kafka/internal/domain"

// Nomes dos tópicos, um por etapa do fluxo, carregando tanto comandos quanto resultados da mesma etapa.
const (
	TopicOrderCreated      = "orders.created"
	TopicOrderStatus       = "orders.status"
	TopicOrderPayment      = "orders.payment"
	TopicOrderInventory    = "orders.inventory"
	TopicOrderNotification = "orders.notification"
)

// topicForEventType centraliza o roteamento de cada tipo de evento para o tópico correspondente à sua etapa.
var topicForEventType = map[domain.EventType]string{
	domain.EventOrderCreated:            TopicOrderCreated,
	domain.EventOrderCompleted:          TopicOrderStatus,
	domain.EventOrderFailed:             TopicOrderStatus,
	domain.EventPaymentCommand:          TopicOrderPayment,
	domain.EventPaymentCompensate:       TopicOrderPayment,
	domain.EventPaymentResult:           TopicOrderPayment,
	domain.EventPaymentCompensateResult: TopicOrderPayment,
	domain.EventInventoryCommand:        TopicOrderInventory,
	domain.EventInventoryResult:         TopicOrderInventory,
	domain.EventNotificationCommand:     TopicOrderNotification,
	domain.EventNotificationResult:      TopicOrderNotification,
}

// DLQTopicFor retorna o tópico de dead letter associado a um tópico de origem.
func DLQTopicFor(topic string) string {
	return topic + ".dlq"
}

// TopicForEventType retorna o tópico do evento e true se o tipo for conhecido.
func TopicForEventType(eventType domain.EventType) (string, bool) {
	topic, ok := topicForEventType[eventType]
	return topic, ok
}
