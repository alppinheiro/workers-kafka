package worker

import (
	"context"
	"fmt"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// InventoryUseCase processa o comando de reserva de estoque e publica o resultado da
// etapa, registrando no journal o request/response do gateway.
type InventoryUseCase struct {
	gateway   application.InventoryGateway
	publisher application.EventPublisher
	eventLog  application.EventLogRepository
}

// NewInventoryUseCase monta o caso de uso do worker de estoque a partir de suas dependências.
func NewInventoryUseCase(gateway application.InventoryGateway, publisher application.EventPublisher, eventLog application.EventLogRepository) *InventoryUseCase {
	return &InventoryUseCase{gateway: gateway, publisher: publisher, eventLog: eventLog}
}

// Handle consome um evento do tópico de estoque, ignorando os que não forem comandos,
// e publica o resultado.
func (uc *InventoryUseCase) Handle(ctx context.Context, event domain.Event) error {
	if event.EventType != domain.EventInventoryCommand {
		return nil // resultados no mesmo tópico não interessam a este worker.
	}

	if event.StatusAtual != domain.StatusPaymentApproved {
		return fmt.Errorf("comando de estoque inválido para o pedido %s: status esperado %s, recebido %s", event.OrderID, domain.StatusPaymentApproved, event.StatusAtual)
	}

	if err := appendLog(ctx, uc.eventLog, componentInventory, event, application.DirectionIn, nil, nil); err != nil {
		return err
	}

	requestPayload := map[string]any{"order_id": event.OrderID, "op": "reserve"}
	if err := appendLog(ctx, uc.eventLog, componentInventory, event, application.DirectionGatewayRequest, requestPayload, nil); err != nil {
		return err
	}

	reserved, err := uc.gateway.Reserve(ctx, event.OrderID)

	responsePayload := map[string]any{"reserved": reserved}
	if err != nil {
		responsePayload = map[string]any{"error": err.Error()}
	}
	if err := appendLog(ctx, uc.eventLog, componentInventory, event, application.DirectionGatewayResponse, nil, responsePayload); err != nil {
		return err
	}

	result := domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        event.OrderID,
		SagaID:         event.SagaID,
		StatusAnterior: event.StatusAtual,
		EventType:      domain.EventInventoryResult,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
	}

	switch {
	case err != nil:
		result.StatusAtual = domain.StatusRetrying
		result.Metadata = map[string]string{"erro": err.Error()}
	case reserved:
		result.StatusAtual = domain.StatusInventoryReserved
	default:
		result.StatusAtual = domain.StatusFailed
	}

	if err := appendLog(ctx, uc.eventLog, componentInventory, result, application.DirectionOut, nil, nil); err != nil {
		return err
	}
	return uc.publisher.Publish(ctx, result)
}
