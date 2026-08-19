package worker

import (
	"context"
	"fmt"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// PaymentUseCase processa o comando de pagamento e publica o resultado da etapa.
type PaymentUseCase struct {
	gateway   application.PaymentGateway
	publisher application.EventPublisher
}

// NewPaymentUseCase monta o caso de uso do worker de pagamento a partir de suas dependências.
func NewPaymentUseCase(gateway application.PaymentGateway, publisher application.EventPublisher) *PaymentUseCase {
	return &PaymentUseCase{gateway: gateway, publisher: publisher}
}

// Handle consome um evento do tópico de pagamento, ignorando os que não forem comandos, e publica o resultado.
func (uc *PaymentUseCase) Handle(ctx context.Context, event domain.Event) error {
	if event.EventType != domain.EventPaymentCommand {
		return nil // resultados no mesmo tópico não interessam a este worker.
	}

	if event.StatusAtual != domain.StatusPaymentPending {
		return fmt.Errorf("comando de pagamento inválido para o pedido %s: status esperado %s, recebido %s", event.OrderID, domain.StatusPaymentPending, event.StatusAtual)
	}

	approved, err := uc.gateway.Process(ctx, event.OrderID)

	result := domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        event.OrderID,
		SagaID:         event.SagaID,
		StatusAnterior: event.StatusAtual,
		EventType:      domain.EventPaymentResult,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
	}

	switch {
	case err != nil:
		result.StatusAtual = domain.StatusRetrying
		result.Metadata = map[string]string{"erro": err.Error()}
	case approved:
		result.StatusAtual = domain.StatusPaymentApproved
	default:
		result.StatusAtual = domain.StatusFailed
	}

	return uc.publisher.Publish(ctx, result)
}
