package worker

import (
	"context"
	"fmt"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// InventoryUseCase processa o comando de reserva de estoque e publica o resultado da etapa.
type InventoryUseCase struct {
	gateway   application.InventoryGateway
	publisher application.EventPublisher
}

// NewInventoryUseCase monta o caso de uso do worker de estoque a partir de suas dependências.
func NewInventoryUseCase(gateway application.InventoryGateway, publisher application.EventPublisher) *InventoryUseCase {
	return &InventoryUseCase{gateway: gateway, publisher: publisher}
}

// Handle consome um evento do tópico de estoque, ignorando os que não forem comandos, e publica o resultado.
func (uc *InventoryUseCase) Handle(ctx context.Context, event domain.Event) error {
	if event.EventType != domain.EventInventoryCommand {
		return nil // resultados no mesmo tópico não interessam a este worker.
	}

	if event.StatusAtual != domain.StatusPaymentApproved {
		return fmt.Errorf("comando de estoque inválido para o pedido %s: status esperado %s, recebido %s", event.OrderID, domain.StatusPaymentApproved, event.StatusAtual)
	}

	reserved, err := uc.gateway.Reserve(ctx, event.OrderID)

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

	return uc.publisher.Publish(ctx, result)
}
