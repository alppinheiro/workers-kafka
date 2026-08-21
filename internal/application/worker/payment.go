package worker

import (
	"context"
	"fmt"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// PaymentUseCase processa o comando de pagamento (e a compensação/estorno) e publica o
// resultado da etapa, registrando no journal o request/response do gateway.
type PaymentUseCase struct {
	gateway   application.PaymentGateway
	publisher application.EventPublisher
	eventLog  application.EventLogRepository
}

// NewPaymentUseCase monta o caso de uso do worker de pagamento a partir de suas dependências.
func NewPaymentUseCase(gateway application.PaymentGateway, publisher application.EventPublisher, eventLog application.EventLogRepository) *PaymentUseCase {
	return &PaymentUseCase{gateway: gateway, publisher: publisher, eventLog: eventLog}
}

// Handle consome um evento do tópico de pagamento, ignorando os que não forem comandos,
// e publica o resultado.
func (uc *PaymentUseCase) Handle(ctx context.Context, event domain.Event) error {
	switch event.EventType {
	case domain.EventPaymentCommand:
		if event.StatusAtual != domain.StatusPaymentPending {
			return fmt.Errorf("%w: comando de pagamento inválido para o pedido %s: status esperado %s, recebido %s", application.ErrNonRetryable, event.OrderID, domain.StatusPaymentPending, event.StatusAtual)
		}
		return uc.process(ctx, event)

	case domain.EventPaymentCompensate:
		// comando de estorno assíncrono
		if event.StatusAtual != domain.StatusPaymentRefundPending {
			return fmt.Errorf("%w: comando de estorno inválido para o pedido %s: status esperado %s, recebido %s", application.ErrNonRetryable, event.OrderID, domain.StatusPaymentRefundPending, event.StatusAtual)
		}
		if event.TransactionID == "" {
			return fmt.Errorf("%w: comando de estorno inválido para o pedido %s: transaction_id ausente", application.ErrNonRetryable, event.OrderID)
		}
		return uc.refund(ctx, event)

	default:
		return nil // eventos não relacionados a pagamento não interessam a este worker
	}
}

func (uc *PaymentUseCase) process(ctx context.Context, event domain.Event) error {
	seen, err := alreadyProcessed(ctx, uc.eventLog, componentPayment, event)
	if err != nil {
		return err
	}
	if seen {
		return nil // comando já processado (redelivery)
	}

	if err := appendLog(ctx, uc.eventLog, componentPayment, event, application.DirectionIn, nil, nil); err != nil {
		return err
	}

	requestPayload := map[string]any{"order_id": event.OrderID, "op": "process"}
	if err := appendLog(ctx, uc.eventLog, componentPayment, event, application.DirectionGatewayRequest, requestPayload, nil); err != nil {
		return err
	}

	approved, txID, err := uc.gateway.Process(ctx, event.OrderID)

	responsePayload := map[string]any{"approved": approved, "transaction_id": txID}
	if err != nil {
		responsePayload = map[string]any{"error": err.Error()}
	}
	if err := appendLog(ctx, uc.eventLog, componentPayment, event, application.DirectionGatewayResponse, nil, responsePayload); err != nil {
		return err
	}

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
		result.TransactionID = txID
	default:
		result.StatusAtual = domain.StatusFailed
	}

	if err := appendLog(ctx, uc.eventLog, componentPayment, result, application.DirectionOut, nil, nil); err != nil {
		return err
	}
	return uc.publisher.Publish(ctx, result)
}

func (uc *PaymentUseCase) refund(ctx context.Context, event domain.Event) error {
	seen, err := alreadyProcessed(ctx, uc.eventLog, componentPayment, event)
	if err != nil {
		return err
	}
	if seen {
		return nil // comando já processado (redelivery)
	}

	if err := appendLog(ctx, uc.eventLog, componentPayment, event, application.DirectionIn, nil, nil); err != nil {
		return err
	}

	requestPayload := map[string]any{"order_id": event.OrderID, "op": "refund", "transaction_id": event.TransactionID}
	if err := appendLog(ctx, uc.eventLog, componentPayment, event, application.DirectionGatewayRequest, requestPayload, nil); err != nil {
		return err
	}

	refunded, err := uc.gateway.Refund(ctx, event.OrderID, event.TransactionID)

	responsePayload := map[string]any{"refunded": refunded}
	if err != nil {
		responsePayload = map[string]any{"error": err.Error()}
	}
	if err := appendLog(ctx, uc.eventLog, componentPayment, event, application.DirectionGatewayResponse, nil, responsePayload); err != nil {
		return err
	}

	result := domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        event.OrderID,
		SagaID:         event.SagaID,
		StatusAnterior: event.StatusAtual,
		EventType:      domain.EventPaymentCompensateResult,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
		TransactionID:  event.TransactionID,
	}

	switch {
	case err != nil:
		result.StatusAtual = domain.StatusRetrying
		result.Metadata = map[string]string{"erro": err.Error()}
	case refunded:
		result.StatusAtual = domain.StatusPaymentRefunded
	default:
		result.StatusAtual = domain.StatusFailed
	}

	if err := appendLog(ctx, uc.eventLog, componentPayment, result, application.DirectionOut, nil, nil); err != nil {
		return err
	}
	return uc.publisher.Publish(ctx, result)
}
