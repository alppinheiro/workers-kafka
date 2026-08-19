package worker

import (
	"context"
	"fmt"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// NotificationUseCase processa o comando de notificação e publica o encerramento da etapa.
type NotificationUseCase struct {
	gateway   application.NotificationGateway
	publisher application.EventPublisher
}

// NewNotificationUseCase monta o caso de uso do worker de notificação a partir de suas dependências.
func NewNotificationUseCase(gateway application.NotificationGateway, publisher application.EventPublisher) *NotificationUseCase {
	return &NotificationUseCase{gateway: gateway, publisher: publisher}
}

// Handle consome um evento do tópico de notificação, ignorando os que não forem comandos, e publica o resultado.
func (uc *NotificationUseCase) Handle(ctx context.Context, event domain.Event) error {
	if event.EventType != domain.EventNotificationCommand {
		return nil // resultados no mesmo tópico não interessam a este worker.
	}

	if event.StatusAtual != domain.StatusInventoryReserved {
		return fmt.Errorf("comando de notificação inválido para o pedido %s: status esperado %s, recebido %s", event.OrderID, domain.StatusInventoryReserved, event.StatusAtual)
	}

	sent, err := uc.gateway.Notify(ctx, event.OrderID)

	result := domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        event.OrderID,
		SagaID:         event.SagaID,
		StatusAnterior: event.StatusAtual,
		EventType:      domain.EventNotificationResult,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
	}

	switch {
	case err != nil:
		result.StatusAtual = domain.StatusRetrying
		result.Metadata = map[string]string{"erro": err.Error()}
	case sent:
		result.StatusAtual = domain.StatusNotified
	default:
		result.StatusAtual = domain.StatusFailed
	}

	return uc.publisher.Publish(ctx, result)
}
