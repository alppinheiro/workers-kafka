package orchestrator

import (
	"context"
	"fmt"
	"log"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// Orchestrator coordena o avanço, retry e encerramento da saga do pedido, sem executar
// regra de negócio das etapas. O estado da saga é persistido em um SagaRepository e cada
// transição é registrada no journal de eventos para rastreabilidade.
type Orchestrator struct {
	publisher  application.EventPublisher
	sagas      application.SagaRepository
	eventLog   application.EventLogRepository
	maxRetries int
}

// New cria um orquestrador com o publisher usado para disparar os próximos comandos, o
// repositório de sagas, o journal de eventos e o limite de tentativas de retry.
func New(publisher application.EventPublisher, sagas application.SagaRepository, eventLog application.EventLogRepository, maxRetries int) *Orchestrator {
	return &Orchestrator{
		publisher:  publisher,
		sagas:      sagas,
		eventLog:   eventLog,
		maxRetries: maxRetries,
	}
}

// StartOrder inicia uma nova saga em PENDING a partir do ORDER_CREATED e dispara o
// primeiro comando do fluxo.
func (o *Orchestrator) StartOrder(ctx context.Context, event domain.Event) error {
	if _, err := o.sagas.Load(ctx, event.OrderID); err == nil {
		return fmt.Errorf("saga já iniciada para o pedido %s", event.OrderID)
	} else if err != application.ErrSagaNotFound {
		return fmt.Errorf("erro ao verificar saga para o pedido %s: %w", event.OrderID, err)
	}

	saga := domain.Saga{OrderID: event.OrderID, Current: domain.StatusPending}

	if err := o.logEvent(ctx, saga, event.EventID, domain.EventOrderCreated, application.DirectionIn, domain.StatusPending, event, nil, nil); err != nil {
		return err
	}
	if err := o.sagas.Save(ctx, saga); err != nil {
		return err
	}

	log.Printf("component=orchestrator phase=decision action=start order_id=%s saga_id=%s state_current=%s retry_count=%d", event.OrderID, event.OrderID, domain.StatusPending, 0)
	return o.dispatchNext(ctx, &saga)
}

// HandleEvent é o ponto único de entrada do orquestrador: inicia a saga na criação do
// pedido e delega os demais eventos para HandleResult.
func (o *Orchestrator) HandleEvent(ctx context.Context, event domain.Event) error {
	if event.EventType == domain.EventOrderCreated {
		log.Printf("component=orchestrator phase=decision action=handle-created order_id=%s saga_id=%s event_id=%s type=%s", event.OrderID, event.SagaID, event.EventID, event.EventType)
		return o.StartOrder(ctx, event)
	}
	return o.HandleResult(ctx, event)
}

// HandleResult reage a um evento de resultado publicado por um dos workers e decide o
// próximo passo da saga, salvando o novo estado antes de publicar.
func (o *Orchestrator) HandleResult(ctx context.Context, event domain.Event) error {
	switch event.EventType {
	case domain.EventPaymentResult, domain.EventPaymentCompensateResult, domain.EventInventoryResult, domain.EventNotificationResult:
		// segue para o processamento do resultado abaixo.
	default:
		return nil // comandos publicados pelo próprio orquestrador não são de seu interesse.
	}

	saga, err := o.sagas.Load(ctx, event.OrderID)
	if err != nil {
		if err == application.ErrSagaNotFound {
			return fmt.Errorf("saga desconhecida para o pedido %s", event.OrderID)
		}
		return fmt.Errorf("erro ao carregar saga para o pedido %s: %w", event.OrderID, err)
	}

	if err := o.logEvent(ctx, saga, event.EventID, event.EventType, application.DirectionIn, event.StatusAtual, event, nil, nil); err != nil {
		return err
	}

	expectedStatus, err := expectedStatusForResult(event.EventType)
	if err != nil {
		return err
	}

	if saga.Current != expectedStatus {
		return fmt.Errorf("evento fora de ordem para o pedido %s: resultado %s exige saga em %s, mas estava em %s", event.OrderID, event.EventType, expectedStatus, saga.Current)
	}

	if err := validateResultStatus(event.EventType, event.StatusAtual); err != nil {
		return fmt.Errorf("resultado inválido para o pedido %s: %w", event.OrderID, err)
	}

	switch event.StatusAtual {
	case domain.StatusRetrying:
		log.Printf("component=orchestrator phase=decision action=retry-requested order_id=%s saga_id=%s event_id=%s type=%s state_current=%s retry_count=%d metadata=%v", event.OrderID, event.SagaID, event.EventID, event.EventType, saga.Current, saga.RetryCount, event.Metadata)
		return o.retry(ctx, &saga)
	case domain.StatusFailed:
		// special-case: notification failures do not change order outcome - treat as completed (notification requested but failed)
		if event.EventType == domain.EventNotificationResult {
			log.Printf("component=orchestrator phase=decision action=notification-failed-ignored order_id=%s saga_id=%s event_id=%s metadata=%v", event.OrderID, event.SagaID, event.EventID, event.Metadata)
			meta := map[string]string{"notification_error": "true"}
			return o.complete(ctx, &saga, meta)
		}

		log.Printf("component=orchestrator phase=decision action=fail-requested order_id=%s saga_id=%s event_id=%s type=%s state_current=%s retry_count=%d metadata=%v", event.OrderID, event.SagaID, event.EventID, event.EventType, saga.Current, saga.RetryCount, event.Metadata)
		if event.EventType == domain.EventInventoryResult && saga.Current == domain.StatusPaymentApproved && saga.TransactionID != "" {
			return o.startCompensation(ctx, &saga)
		}

		if event.EventType == domain.EventPaymentCompensateResult {
			metadata := cloneMetadata(event.Metadata)
			metadata["payment_refund_failed"] = "true"
			metadata["transaction_id"] = event.TransactionID
			return o.fail(ctx, &saga, metadata)
		}
		return o.fail(ctx, &saga, event.Metadata)
	case domain.StatusPaymentApproved, domain.StatusInventoryReserved:
		log.Printf("component=orchestrator phase=decision action=advance order_id=%s saga_id=%s event_id=%s type=%s from=%s to=%s retry_count=%d", event.OrderID, event.SagaID, event.EventID, event.EventType, saga.Current, event.StatusAtual, saga.RetryCount)
		saga.Previous = saga.Current
		saga.Current = event.StatusAtual
		saga.RetryCount = 0
		// if payment was approved, persist the transaction id for potential future compensation
		if event.EventType == domain.EventPaymentResult {
			saga.TransactionID = event.TransactionID
		}
		if err := o.sagas.Save(ctx, saga); err != nil {
			return err
		}
		return o.dispatchNext(ctx, &saga)
	case domain.StatusPaymentRefunded:
		log.Printf("component=orchestrator phase=decision action=refund-completed order_id=%s saga_id=%s event_id=%s tx=%s", event.OrderID, event.SagaID, event.EventID, event.TransactionID)
		metadata := cloneMetadata(event.Metadata)
		metadata["payment_refunded"] = "true"
		metadata["transaction_id"] = event.TransactionID
		return o.fail(ctx, &saga, metadata)
	case domain.StatusNotified:
		log.Printf("component=orchestrator phase=decision action=complete-requested order_id=%s saga_id=%s event_id=%s type=%s state_current=%s", event.OrderID, event.SagaID, event.EventID, event.EventType, saga.Current)
		return o.complete(ctx, &saga, nil)
	default:
		return fmt.Errorf("status inesperado recebido pelo orquestrador para o pedido %s: %s", event.OrderID, event.StatusAtual)
	}
}

// retry contabiliza uma nova tentativa e reenvia o mesmo comando, ou encerra a saga
// como FAILED se o limite de tentativas for excedido.
func (o *Orchestrator) retry(ctx context.Context, saga *domain.Saga) error {
	saga.RetryCount++
	if err := o.sagas.Save(ctx, *saga); err != nil {
		return err
	}
	log.Printf("component=orchestrator phase=decision action=retrying order_id=%s saga_id=%s state_current=%s retry_count=%d", saga.OrderID, saga.OrderID, saga.Current, saga.RetryCount)
	if saga.RetryCount > o.maxRetries {
		metadata := map[string]string{"motivo": "retry_limit_exceeded"}
		log.Printf("component=orchestrator phase=decision action=retry-limit-exceeded order_id=%s saga_id=%s state_current=%s retry_count=%d", saga.OrderID, saga.OrderID, saga.Current, saga.RetryCount)
		// special-case: falhas de notificação não derrubam o pedido mesmo após esgotar o retry.
		if saga.Current == domain.StatusInventoryReserved {
			meta := map[string]string{"notification_error": "true", "motivo": "retry_limit_exceeded"}
			return o.complete(ctx, saga, meta)
		}
		if saga.Current == domain.StatusPaymentRefundPending {
			metadata["payment_refund_failed"] = "true"
			metadata["transaction_id"] = saga.TransactionID
		}
		return o.fail(ctx, saga, metadata)
	}
	return o.dispatchNext(ctx, saga)
}

// startCompensation inicia o estorno do pagamento quando uma etapa posterior falha após
// a aprovação, salvando o novo estado antes de publicar o comando de compensação.
func (o *Orchestrator) startCompensation(ctx context.Context, saga *domain.Saga) error {
	saga.Previous = saga.Current
	saga.Current = domain.StatusPaymentRefundPending
	saga.RetryCount = 0
	if err := o.sagas.Save(ctx, *saga); err != nil {
		return err
	}
	log.Printf("component=orchestrator phase=decision action=start-compensation order_id=%s saga_id=%s tx=%s", saga.OrderID, saga.OrderID, saga.TransactionID)
	return o.dispatchNext(ctx, saga)
}

// fail encerra a saga com FAILED e publica um evento terminal para rastreabilidade externa.
func (o *Orchestrator) fail(ctx context.Context, saga *domain.Saga, metadata map[string]string) error {
	previousStatus := saga.Current
	saga.Previous = previousStatus
	saga.Current = domain.StatusFailed
	saga.RetryCount = 0
	if err := o.sagas.Save(ctx, *saga); err != nil {
		return err
	}
	log.Printf("component=orchestrator phase=decision action=failed order_id=%s saga_id=%s from=%s to=%s metadata=%v", saga.OrderID, saga.OrderID, previousStatus, domain.StatusFailed, metadata)

	terminal := terminalEvent(saga.OrderID, previousStatus, domain.StatusFailed, domain.EventOrderFailed, metadata)
	if err := o.logEvent(ctx, *saga, terminal.EventID, domain.EventOrderFailed, application.DirectionOut, domain.StatusFailed, terminal, nil, nil); err != nil {
		return err
	}
	return o.publisher.Publish(ctx, terminal)
}

// complete encerra a saga em COMPLETED após receber a confirmação NOTIFIED do worker final.
// O metadata opcional pode conter informações sobre falhas não-fatais (ex.: notificações).
func (o *Orchestrator) complete(ctx context.Context, saga *domain.Saga, metadata map[string]string) error {
	saga.Previous = saga.Current
	saga.Current = domain.StatusNotified
	saga.RetryCount = 0
	if err := o.sagas.Save(ctx, *saga); err != nil {
		return err
	}
	log.Printf("component=orchestrator phase=decision action=publishing-completed order_id=%s saga_id=%s from=%s to=%s metadata=%v", saga.OrderID, saga.OrderID, domain.StatusNotified, domain.StatusCompleted, metadata)

	terminal := terminalEvent(saga.OrderID, domain.StatusNotified, domain.StatusCompleted, domain.EventOrderCompleted, metadata)
	if err := o.logEvent(ctx, *saga, terminal.EventID, domain.EventOrderCompleted, application.DirectionOut, domain.StatusCompleted, terminal, nil, nil); err != nil {
		return err
	}
	if err := o.publisher.Publish(ctx, terminal); err != nil {
		return err
	}

	saga.Previous = domain.StatusNotified
	saga.Current = domain.StatusCompleted
	if err := o.sagas.Save(ctx, *saga); err != nil {
		return err
	}
	log.Printf("component=orchestrator phase=decision action=completed order_id=%s saga_id=%s state_current=%s", saga.OrderID, saga.OrderID, saga.Current)
	return nil
}

// dispatchNext publica o comando correspondente à etapa seguinte ao status atual da saga,
// salvando o novo estado e registrando o comando no journal antes de publicar.
func (o *Orchestrator) dispatchNext(ctx context.Context, saga *domain.Saga) error {
	targetStatus, eventType, err := nextCommand(saga.Current)
	if err != nil {
		return err
	}

	command := domain.Event{
		EventID:        domain.NewEventID(),
		OrderID:        saga.OrderID,
		SagaID:         saga.OrderID,
		TransactionID:  saga.TransactionID,
		StatusAnterior: saga.Previous,
		StatusAtual:    targetStatus,
		EventType:      eventType,
		SchemaVersion:  domain.CurrentSchemaVersion,
		CreatedAt:      time.Now().UTC(),
	}

	if targetStatus != saga.Current {
		saga.Previous = saga.Current
		saga.Current = targetStatus
	}

	if err := o.sagas.Save(ctx, *saga); err != nil {
		return err
	}
	if err := o.logEvent(ctx, *saga, command.EventID, eventType, application.DirectionOut, targetStatus, command, nil, nil); err != nil {
		return err
	}

	log.Printf("component=orchestrator phase=decision action=dispatch-next order_id=%s saga_id=%s event_type=%s status_previous=%s status_current=%s retry_count=%d", saga.OrderID, saga.OrderID, eventType, command.StatusAnterior, command.StatusAtual, saga.RetryCount)

	return o.publisher.Publish(ctx, command)
}

// logEvent registra a visão do orquestrador sobre um evento no journal (append-only).
func (o *Orchestrator) logEvent(ctx context.Context, saga domain.Saga, eventID string, eventType domain.EventType, direction string, status domain.OrderStatus, payload, requestPayload, responsePayload any) error {
	if o.eventLog == nil {
		return nil
	}
	return o.eventLog.Append(ctx, application.EventLogEntry{
		OrderID:         saga.OrderID,
		SagaID:          saga.OrderID,
		EventID:         eventID,
		EventType:       eventType,
		Component:       "orchestrator",
		Direction:       direction,
		StatusAnterior:  saga.Previous,
		StatusAtual:     status,
		Payload:         payload,
		RequestPayload:  requestPayload,
		ResponsePayload: responsePayload,
	})
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
