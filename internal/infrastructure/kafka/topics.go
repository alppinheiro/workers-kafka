package kafka

import "workers-kafka/internal/domain"

// Nomes dos tópicos do barramento, segregados por direção de mensagem (review 2.6/3.1):
//   - .cmd   → comandos que o orquestrador publica para os workers consumirem;
//   - .result→ resultados que os workers publicam de volta para o orquestrador.
//
// Cada consumer assina apenas o que consome (sem amplificação de mensagens) e os
// contratos de comando/resultado evoluem de forma independente.
const (
	TopicOrderCreated        = "orders.created"
	TopicOrderStatus         = "orders.status"
	TopicPaymentCommand      = "orders.payment.cmd"
	TopicPaymentResult       = "orders.payment.result"
	TopicInventoryCommand    = "orders.inventory.cmd"
	TopicInventoryResult     = "orders.inventory.result"
	TopicNotificationCommand = "orders.notification.cmd"
	TopicNotificationResult  = "orders.notification.result"
)

// topicForEventType centraliza o roteamento de cada tipo de evento para o tópico
// correspondente à sua direção (comando ou resultado da etapa).
var topicForEventType = map[domain.EventType]string{
	domain.EventOrderCreated:            TopicOrderCreated,
	domain.EventOrderCompleted:          TopicOrderStatus,
	domain.EventOrderFailed:             TopicOrderStatus,
	domain.EventPaymentCommand:          TopicPaymentCommand,
	domain.EventPaymentCompensate:       TopicPaymentCommand,
	domain.EventPaymentResult:           TopicPaymentResult,
	domain.EventPaymentCompensateResult: TopicPaymentResult,
	domain.EventInventoryCommand:        TopicInventoryCommand,
	domain.EventInventoryResult:         TopicInventoryResult,
	domain.EventNotificationCommand:     TopicNotificationCommand,
	domain.EventNotificationResult:      TopicNotificationResult,
}

// FlowTopics retorna todos os tópicos do barramento (útil para o projector e o
// metrics-exporter, que acompanham o fluxo completo).
func FlowTopics() []string {
	return []string{
		TopicOrderCreated,
		TopicOrderStatus,
		TopicPaymentCommand,
		TopicPaymentResult,
		TopicInventoryCommand,
		TopicInventoryResult,
		TopicNotificationCommand,
		TopicNotificationResult,
	}
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
