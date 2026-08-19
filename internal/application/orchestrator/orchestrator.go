package orchestrator

import (
	"context"
	"fmt"
	"log"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// sagaState guarda o estado lógico de uma saga em andamento, mantido apenas em memória nesta fase.
type sagaState struct {
	previous      domain.OrderStatus
	current       domain.OrderStatus
	retryCount    int
	transactionID string
}

// Orchestrator coordena o avanço, retry e encerramento da saga do pedido, sem executar regra de negócio das etapas.
type Orchestrator struct {
	publisher  application.EventPublisher
	states     map[string]*sagaState
	maxRetries int
}

// New cria um orquestrador com o publisher usado para disparar os próximos comandos e o limite de tentativas de retry.
func New(publisher application.EventPublisher, maxRetries int) *Orchestrator {
	return &Orchestrator{
		publisher:  publisher,
		states:     make(map[string]*sagaState),
		maxRetries: maxRetries,
	}
}

// StartOrder inicia uma nova saga em PENDING e dispara o primeiro comando do fluxo.
func (o *Orchestrator) StartOrder(ctx context.Context, orderID string) error {
	if _, exists := o.states[orderID]; exists {
		return fmt.Errorf("saga já iniciada para o pedido %s", orderID)
	}

	o.states[orderID] = &sagaState{current: domain.StatusPending}
	log.Printf("component=orchestrator phase=decision action=start order_id=%s saga_id=%s state_current=%s retry_count=%d", orderID, orderID, domain.StatusPending, 0)
	return o.dispatchNext(ctx, orderID)
}

// HandleEvent é o ponto único de entrada do orquestrador: inicia a saga na criação do pedido e
// delega os demais eventos para HandleResult.
func (o *Orchestrator) HandleEvent(ctx context.Context, event domain.Event) error {
	if event.EventType == domain.EventOrderCreated {
		log.Printf("component=orchestrator phase=decision action=handle-created order_id=%s saga_id=%s event_id=%s type=%s", event.OrderID, event.SagaID, event.EventID, event.EventType)
		return o.StartOrder(ctx, event.OrderID)
	}
	return o.HandleResult(ctx, event)
}

// HandleResult reage a um evento de resultado publicado por um dos workers e decide o próximo passo da saga.
func (o *Orchestrator) HandleResult(ctx context.Context, event domain.Event) error {
	switch event.EventType {
	case domain.EventPaymentResult, domain.EventPaymentCompensateResult, domain.EventInventoryResult, domain.EventNotificationResult:
		// segue para o processamento do resultado abaixo.
	default:
		return nil // comandos publicados pelo próprio orquestrador não são de seu interesse.
	}

	state, ok := o.states[event.OrderID]
	if !ok {
		return fmt.Errorf("saga desconhecida para o pedido %s", event.OrderID)
	}

	expectedStatus, err := expectedStatusForResult(event.EventType)
	if err != nil {
		return err
	}

	if state.current != expectedStatus {
		return fmt.Errorf("evento fora de ordem para o pedido %s: resultado %s exige saga em %s, mas estava em %s", event.OrderID, event.EventType, expectedStatus, state.current)
	}

	if err := validateResultStatus(event.EventType, event.StatusAtual); err != nil {
		return fmt.Errorf("resultado inválido para o pedido %s: %w", event.OrderID, err)
	}

	switch event.StatusAtual {
	case domain.StatusRetrying:
		log.Printf("component=orchestrator phase=decision action=retry-requested order_id=%s saga_id=%s event_id=%s type=%s state_current=%s retry_count=%d metadata=%v", event.OrderID, event.SagaID, event.EventID, event.EventType, state.current, state.retryCount, event.Metadata)
		return o.retry(ctx, event.OrderID, state)
	case domain.StatusFailed:
		// special-case: notification failures do not change order outcome - treat as completed (notification requested but failed)
		if event.EventType == domain.EventNotificationResult {
			log.Printf("component=orchestrator phase=decision action=notification-failed-ignored order_id=%s saga_id=%s event_id=%s metadata=%v", event.OrderID, event.SagaID, event.EventID, event.Metadata)
			// mark saga completed even if notification failed; include metadata to signal notification problem
			meta := map[string]string{"notification_error": "true"}
			return o.complete(ctx, event.OrderID, state, meta)
		}

		// special-case: if inventory failed but payment was previously approved, request async compensation
		log.Printf("component=orchestrator phase=decision action=fail-requested order_id=%s saga_id=%s event_id=%s type=%s state_current=%s retry_count=%d metadata=%v", event.OrderID, event.SagaID, event.EventID, event.EventType, state.current, state.retryCount, event.Metadata)
		if event.EventType == domain.EventInventoryResult && state.current == domain.StatusPaymentApproved && state.transactionID != "" {
			return o.startCompensation(ctx, event.OrderID, state)
		}

		if event.EventType == domain.EventPaymentCompensateResult {
			metadata := cloneMetadata(event.Metadata)
			metadata["payment_refund_failed"] = "true"
			metadata["transaction_id"] = event.TransactionID
			return o.fail(ctx, event.OrderID, state, metadata)
		}
		return o.fail(ctx, event.OrderID, state, event.Metadata)
	case domain.StatusPaymentApproved, domain.StatusInventoryReserved:
		log.Printf("component=orchestrator phase=decision action=advance order_id=%s saga_id=%s event_id=%s type=%s from=%s to=%s retry_count=%d", event.OrderID, event.SagaID, event.EventID, event.EventType, state.current, event.StatusAtual, state.retryCount)
		state.previous = state.current
		state.current = event.StatusAtual
		state.retryCount = 0
		// if payment was approved, persist the transaction id for potential future compensation
		if event.EventType == domain.EventPaymentResult {
			state.transactionID = event.TransactionID
		}
		return o.dispatchNext(ctx, event.OrderID)
	case domain.StatusPaymentRefunded:
		log.Printf("component=orchestrator phase=decision action=refund-completed order_id=%s saga_id=%s event_id=%s tx=%s", event.OrderID, event.SagaID, event.EventID, event.TransactionID)
		metadata := cloneMetadata(event.Metadata)
		metadata["payment_refunded"] = "true"
		metadata["transaction_id"] = event.TransactionID
		return o.fail(ctx, event.OrderID, state, metadata)
	case domain.StatusNotified:
		log.Printf("component=orchestrator phase=decision action=complete-requested order_id=%s saga_id=%s event_id=%s type=%s state_current=%s", event.OrderID, event.SagaID, event.EventID, event.EventType, state.current)
		return o.complete(ctx, event.OrderID, state, nil)
	default:
		return fmt.Errorf("status inesperado recebido pelo orquestrador para o pedido %s: %s", event.OrderID, event.StatusAtual)
	}
}

// retry contabiliza uma nova tentativa e reenvia o mesmo comando, ou encerra a saga como FAILED se o limite estourar.
func (o *Orchestrator) retry(ctx context.Context, orderID string, state *sagaState) error {
	state.retryCount++
	log.Printf("component=orchestrator phase=decision action=retrying order_id=%s saga_id=%s state_current=%s retry_count=%d", orderID, orderID, state.current, state.retryCount)
	if state.retryCount > o.maxRetries {
		metadata := map[string]string{"motivo": "retry_limit_exceeded"}
		log.Printf("component=orchestrator phase=decision action=retry-limit-exceeded order_id=%s saga_id=%s state_current=%s retry_count=%d", orderID, orderID, state.current, state.retryCount)
		// special-case: if we're in notification stage, do not fail the order on exhausted retries
		if state.current == domain.StatusInventoryReserved {
			meta := map[string]string{"notification_error": "true", "motivo": "retry_limit_exceeded"}
			return o.complete(ctx, orderID, state, meta)
		}
		if state.current == domain.StatusPaymentRefundPending {
			metadata["payment_refund_failed"] = "true"
			metadata["transaction_id"] = state.transactionID
		}
		return o.fail(ctx, orderID, state, metadata)
	}
	return o.dispatchNext(ctx, orderID)
}

func (o *Orchestrator) startCompensation(ctx context.Context, orderID string, state *sagaState) error {
	state.previous = state.current
	state.current = domain.StatusPaymentRefundPending
	state.retryCount = 0
	log.Printf("component=orchestrator phase=decision action=start-compensation order_id=%s saga_id=%s tx=%s", orderID, orderID, state.transactionID)
	return o.dispatchNext(ctx, orderID)
}

// fail encerra a saga com FAILED e publica um evento terminal para rastreabilidade externa.
func (o *Orchestrator) fail(ctx context.Context, orderID string, state *sagaState, metadata map[string]string) error {
	previousStatus := state.current
	state.previous = previousStatus
	state.current = domain.StatusFailed
	state.retryCount = 0
	log.Printf("component=orchestrator phase=decision action=failed order_id=%s saga_id=%s from=%s to=%s metadata=%v", orderID, orderID, previousStatus, domain.StatusFailed, metadata)

	return o.publisher.Publish(ctx, terminalEvent(orderID, previousStatus, domain.StatusFailed, domain.EventOrderFailed, metadata))
}

// complete encerra a saga em COMPLETED após receber a confirmação NOTIFIED do worker final.
// metadata opcional pode conter informações sobre falhas não-fatais (ex.: notificações).
func (o *Orchestrator) complete(ctx context.Context, orderID string, state *sagaState, metadata map[string]string) error {
	state.previous = state.current
	state.current = domain.StatusNotified
	state.retryCount = 0
	log.Printf("component=orchestrator phase=decision action=publishing-completed order_id=%s saga_id=%s from=%s to=%s metadata=%v", orderID, orderID, domain.StatusNotified, domain.StatusCompleted, metadata)

	if err := o.publisher.Publish(ctx, terminalEvent(orderID, domain.StatusNotified, domain.StatusCompleted, domain.EventOrderCompleted, metadata)); err != nil {
		return err
	}

	state.previous = domain.StatusNotified
	state.current = domain.StatusCompleted
	log.Printf("component=orchestrator phase=decision action=completed order_id=%s saga_id=%s state_current=%s", orderID, orderID, state.current)
	return nil
}

// dispatchNext publica o comando correspondente à etapa seguinte ao status atual da saga.
// Reenviar o mesmo comando em caso de retry é seguro, pois o status alvo já reflete a etapa em andamento.
func (o *Orchestrator) dispatchNext(ctx context.Context, orderID string) error {
	state, ok := o.states[orderID]
	if !ok {
		return fmt.Errorf("saga desconhecida para o pedido %s", orderID)
	}

	targetStatus, eventType, err := nextCommand(state.current)
	if err != nil {
		return err
	}

	command := domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        orderID,
		SagaID:         orderID,
		TransactionID:  state.transactionID,
		StatusAnterior: state.previous,
		StatusAtual:    targetStatus,
		EventType:      eventType,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
	}

	if targetStatus != state.current {
		state.previous = state.current
		state.current = targetStatus
	}

	log.Printf("component=orchestrator phase=decision action=dispatch-next order_id=%s saga_id=%s event_type=%s status_previous=%s status_current=%s retry_count=%d", orderID, orderID, eventType, command.StatusAnterior, command.StatusAtual, state.retryCount)

	return o.publisher.Publish(ctx, command)
}

// nextCommand mapeia o status atual da saga para o status alvo e o tipo de comando que aciona o próximo worker.
// PAYMENT_PENDING é um caso especial: é o único status intermediário de comando previsto no fluxo, os demais
// workers (estoque e notificação) são acionados reaproveitando o último status já confirmado.
func nextCommand(current domain.OrderStatus) (domain.OrderStatus, domain.EventType, error) {
	switch current {
	case domain.StatusPending, domain.StatusPaymentPending:
		return domain.StatusPaymentPending, domain.EventPaymentCommand, nil
	case domain.StatusPaymentApproved:
		return domain.StatusPaymentApproved, domain.EventInventoryCommand, nil
	case domain.StatusPaymentRefundPending:
		return domain.StatusPaymentRefundPending, domain.EventPaymentCompensate, nil
	case domain.StatusInventoryReserved:
		return domain.StatusInventoryReserved, domain.EventNotificationCommand, nil
	default:
		return "", "", fmt.Errorf("não há próxima etapa definida para o status %s", current)
	}
}

// expectedStatusForResult informa em qual etapa a saga deve estar para aceitar cada resultado.
func expectedStatusForResult(eventType domain.EventType) (domain.OrderStatus, error) {
	switch eventType {
	case domain.EventPaymentResult:
		return domain.StatusPaymentPending, nil
	case domain.EventPaymentCompensateResult:
		return domain.StatusPaymentRefundPending, nil
	case domain.EventInventoryResult:
		return domain.StatusPaymentApproved, nil
	case domain.EventNotificationResult:
		return domain.StatusInventoryReserved, nil
	default:
		return "", fmt.Errorf("tipo de evento não suportado pelo orquestrador: %s", eventType)
	}
}

// validateResultStatus restringe os status válidos para o resultado de cada worker.
func validateResultStatus(eventType domain.EventType, status domain.OrderStatus) error {
	var valid bool

	switch eventType {
	case domain.EventPaymentResult:
		valid = status == domain.StatusPaymentApproved || status == domain.StatusRetrying || status == domain.StatusFailed
	case domain.EventPaymentCompensateResult:
		valid = status == domain.StatusPaymentRefunded || status == domain.StatusRetrying || status == domain.StatusFailed
	case domain.EventInventoryResult:
		valid = status == domain.StatusInventoryReserved || status == domain.StatusRetrying || status == domain.StatusFailed
	case domain.EventNotificationResult:
		valid = status == domain.StatusNotified || status == domain.StatusRetrying || status == domain.StatusFailed
	}

	if valid {
		return nil
	}

	return fmt.Errorf("status %s não é compatível com o resultado %s", status, eventType)
}

func cloneMetadata(metadata map[string]string) map[string]string {
	cloned := make(map[string]string, len(metadata)+2)
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

// terminalEvent padroniza a publicação de eventos finais da saga em um tópico próprio.
func terminalEvent(orderID string, previousStatus domain.OrderStatus, currentStatus domain.OrderStatus, eventType domain.EventType, metadata map[string]string) domain.Event {
	return domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        orderID,
		SagaID:         orderID,
		StatusAnterior: previousStatus,
		StatusAtual:    currentStatus,
		EventType:      eventType,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
		Metadata:       metadata,
	}
}
