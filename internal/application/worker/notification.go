package worker

import (
	"context"
	"fmt"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// NotificationUseCase processa o comando de notificação e publica o encerramento da
// etapa, registrando no journal o request/response do gateway.
type NotificationUseCase struct {
	gateway   application.NotificationGateway
	publisher application.EventPublisher
	eventLog  application.EventLogRepository
}

// NewNotificationUseCase monta o caso de uso do worker de notificação a partir de suas dependências.
func NewNotificationUseCase(gateway application.NotificationGateway, publisher application.EventPublisher, eventLog application.EventLogRepository) *NotificationUseCase {
	return &NotificationUseCase{gateway: gateway, publisher: publisher, eventLog: eventLog}
}

// Handle consome um evento do tópico de notificação, ignorando os que não forem comandos,
// e publica o resultado.
func (uc *NotificationUseCase) Handle(ctx context.Context, event domain.Event) error {
	if event.EventType != domain.EventNotificationCommand {
		return nil // resultados no mesmo tópico não interessam a este worker.
	}

	if event.StatusAtual != domain.StatusInventoryReserved {
		return fmt.Errorf("comando de notificação inválido para o pedido %s: status esperado %s, recebido %s", event.OrderID, domain.StatusInventoryReserved, event.StatusAtual)
	}

	if err := appendLog(ctx, uc.eventLog, componentNotification, event, application.DirectionIn, nil, nil); err != nil {
		return err
	}

	requestPayload := map[string]any{"order_id": event.OrderID, "op": "notify"}
	if err := appendLog(ctx, uc.eventLog, componentNotification, event, application.DirectionGatewayRequest, requestPayload, nil); err != nil {
		return err
	}

	sent, err := uc.gateway.Notify(ctx, event.OrderID)

	responsePayload := map[string]any{"sent": sent}
	if err != nil {
		responsePayload = map[string]any{"error": err.Error()}
	}
	if err := appendLog(ctx, uc.eventLog, componentNotification, event, application.DirectionGatewayResponse, nil, responsePayload); err != nil {
		return err
	}

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

	if err := appendLog(ctx, uc.eventLog, componentNotification, result, application.DirectionOut, nil, nil); err != nil {
		return err
	}
	return uc.publisher.Publish(ctx, result)
}
