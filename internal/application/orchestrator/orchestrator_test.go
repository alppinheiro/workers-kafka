package orchestrator

import (
	"context"
	"testing"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// --- helpers de teste -------------------------------------------------------

// newTestOrchestrator cria um orquestrador com mock publisher, repositório de sagas
// em memória e journal fake, com limite de retries configurável.
func newTestOrchestrator(maxRetries int) (*Orchestrator, *mockPublisher) {
	pub := &mockPublisher{}
	return New(pub, newInMemorySagaRepo(), &fakeEventLog{}, maxRetries), pub
}

// resultEvent cria um evento de resultado com valores mínimos para os testes.
func resultEvent(orderID string, eventType domain.EventType, status domain.OrderStatus) domain.Event {
	return domain.Event{
		EventID:       "evt-1",
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   status,
		EventType:     eventType,
		SchemaVersion: domain.CurrentSchemaVersion,
	}
}

// hasEventType verifica se algum evento publicado tem o tipo esperado.
func hasEventType(events []domain.Event, eventType domain.EventType) bool {
	for _, e := range events {
		if e.EventType == eventType {
			return true
		}
	}
	return false
}

func lastEvent(events []domain.Event) domain.Event {
	return events[len(events)-1]
}

// startOrder dispara a criação de uma saga a partir de um ORDER_CREATED.
func startOrder(t *testing.T, o *Orchestrator, orderID string) {
	t.Helper()
	if err := o.StartOrder(context.Background(), resultEvent(orderID, domain.EventOrderCreated, domain.StatusPending)); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}
}

// seedState grava um estado inicial diretamente no repositório de sagas.
func seedState(t *testing.T, o *Orchestrator, saga domain.Saga) {
	t.Helper()
	if err := o.sagas.Save(context.Background(), saga); err != nil {
		t.Fatalf("seedState falhou: %v", err)
	}
}

// currentState carrega o estado corrente da saga a partir do repositório.
func currentState(t *testing.T, o *Orchestrator, orderID string) domain.Saga {
	t.Helper()
	saga, err := o.sagas.Load(context.Background(), orderID)
	if err != nil {
		t.Fatalf("Load falhou: %v", err)
	}
	return saga
}

// --- StartOrder --------------------------------------------------------------

func TestStartOrder_Success(t *testing.T) {
	o, pub := newTestOrchestrator(3)

	if err := o.StartOrder(context.Background(), resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)); err != nil {
		t.Fatalf("StartOrder retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusPaymentPending {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentPending, state.Current)
	}
	if state.Previous != domain.StatusPending {
		t.Errorf("estado anterior esperado %s, obtido %s", domain.StatusPending, state.Previous)
	}
	if len(pub.events) != 1 {
		t.Fatalf("esperado 1 comando publicado, obtido %d", len(pub.events))
	}
	if !hasEventType(pub.events, domain.EventPaymentCommand) {
		t.Errorf("comando esperado %s, obtidos %v", domain.EventPaymentCommand, pub.events)
	}
}

func TestStartOrder_Duplicate(t *testing.T) {
	o, _ := newTestOrchestrator(3)

	if err := o.StartOrder(context.Background(), resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)); err != nil {
		t.Fatalf("primeira chamada falhou: %v", err)
	}

	err := o.StartOrder(context.Background(), resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending))
	if err == nil {
		t.Fatal("esperado erro para saga já iniciada")
	}
}

// --- HandleEvent -------------------------------------------------------------

func TestHandleEvent_OrderCreated_StartsSaga(t *testing.T) {
	o, _ := newTestOrchestrator(3)

	event := resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)
	err := o.HandleEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleEvent retornou erro inesperado: %v", err)
	}

	if _, err := o.sagas.Load(context.Background(), "order-001"); err != nil {
		t.Fatal("saga não foi iniciada a partir do ORDER_CREATED")
	}
}

func TestHandleEvent_CommandType_Ignored(t *testing.T) {
	o, pub := newTestOrchestrator(3)

	// comando publicado pelo próprio orquestrador não é de seu interesse
	event := resultEvent("order-001", domain.EventPaymentCommand, domain.StatusPaymentPending)
	err := o.HandleEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleEvent retornou erro inesperado: %v", err)
	}

	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

func TestHandleEvent_OrderCreated_DuplicateSaga_ReturnsError(t *testing.T) {
	o, _ := newTestOrchestrator(3)

	created := resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)
	if err := o.HandleEvent(context.Background(), created); err != nil {
		t.Fatalf("primeiro ORDER_CREATED falhou: %v", err)
	}

	err := o.HandleEvent(context.Background(), created)
	if err == nil {
		t.Fatal("esperado erro ao criar saga duplicada")
	}
}

// --- HandleResult: avanço normal ---------------------------------------------

func TestHandleResult_PaymentApproved_AdvancesAndDispatchesInventory(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	startOrder(t, o, "order-001")

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	event.TransactionID = "tx-123"
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusPaymentApproved {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentApproved, state.Current)
	}
	if state.TransactionID != "tx-123" {
		t.Errorf("transactionID esperado tx-123, obtido %s", state.TransactionID)
	}
	if !hasEventType(pub.events, domain.EventInventoryCommand) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventInventoryCommand, pub.events)
	}
}

func TestHandleResult_InventoryReserved_AdvancesAndDispatchesNotification(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentApproved, TransactionID: "tx-123"})

	event := resultEvent("order-001", domain.EventInventoryResult, domain.StatusInventoryReserved)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusInventoryReserved {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusInventoryReserved, state.Current)
	}
	if !hasEventType(pub.events, domain.EventNotificationCommand) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventNotificationCommand, pub.events)
	}
}

// --- HandleResult: retry -----------------------------------------------------

func TestHandleResult_Retrying_RedispatchesSameCommand(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusRetrying)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.RetryCount != 1 {
		t.Errorf("retryCount esperado 1, obtido %d", state.RetryCount)
	}
	if !hasEventType(pub.events, domain.EventPaymentCommand) {
		t.Errorf("esperado novo comando %s, obtidos %v", domain.EventPaymentCommand, pub.events)
	}
}

func TestHandleResult_RetryExceededOnPayment_FailsOrder(t *testing.T) {
	o, pub := newTestOrchestrator(2)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending, RetryCount: 2})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusRetrying)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["motivo"] != "retry_limit_exceeded" {
		t.Errorf("metadata motivo esperado retry_limit_exceeded, obtido %v", meta)
	}
}

func TestHandleResult_RetryExceededOnNotification_CompletesOrder(t *testing.T) {
	o, pub := newTestOrchestrator(2)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusInventoryReserved, RetryCount: 2})

	event := resultEvent("order-001", domain.EventNotificationResult, domain.StatusRetrying)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusCompleted {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusCompleted, state.Current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["notification_error"] != "true" {
		t.Errorf("metadata notification_error esperado true, obtido %v", meta)
	}
}

// --- HandleResult: falha e compensação ---------------------------------------

func TestHandleResult_InventoryFailed_TriggersCompensation(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentApproved, TransactionID: "tx-123"})

	event := resultEvent("order-001", domain.EventInventoryResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusPaymentRefundPending {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentRefundPending, state.Current)
	}
	if !hasEventType(pub.events, domain.EventPaymentCompensate) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventPaymentCompensate, pub.events)
	}
}

func TestHandleResult_PaymentFailed_FailsOrder(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
}

func TestHandleResult_NotificationFailed_StillCompletes(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusInventoryReserved})

	event := resultEvent("order-001", domain.EventNotificationResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusCompleted {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusCompleted, state.Current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["notification_error"] != "true" {
		t.Errorf("metadata notification_error esperado true, obtido %v", meta)
	}
}

func TestHandleResult_InventoryFailedWithoutTransactionID_FailsOrder(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentApproved}) // sem transactionID

	event := resultEvent("order-001", domain.EventInventoryResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
}

// --- HandleResult: erros de validação ----------------------------------------

func TestHandleResult_UnknownSaga(t *testing.T) {
	o, _ := newTestOrchestrator(3)

	event := resultEvent("order-desconhecida", domain.EventPaymentResult, domain.StatusPaymentApproved)
	if err := o.HandleResult(context.Background(), event); err == nil {
		t.Fatal("esperado erro para saga desconhecida")
	}
}

func TestHandleResult_OutOfOrderEvent(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusInventoryReserved})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	if err := o.HandleResult(context.Background(), event); err == nil {
		t.Fatal("esperado erro para evento fora de ordem")
	}
}

func TestHandleResult_InvalidResultStatus(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusCompleted)
	if err := o.HandleResult(context.Background(), event); err == nil {
		t.Fatal("esperado erro para status inválido no resultado")
	}
}

// --- HandleResult: estorno ---------------------------------------------------

func TestHandleResult_Refunded_FailsWithRefundMetadata(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentRefundPending, TransactionID: "tx-123"})

	event := resultEvent("order-001", domain.EventPaymentCompensateResult, domain.StatusPaymentRefunded)
	event.TransactionID = "tx-123"
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["payment_refunded"] != "true" {
		t.Errorf("metadata payment_refunded esperado true, obtido %v", meta)
	}
}

func TestHandleResult_RefundFailed_FailsWithRefundFailedMetadata(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentRefundPending, TransactionID: "tx-123"})

	event := resultEvent("order-001", domain.EventPaymentCompensateResult, domain.StatusFailed)
	event.TransactionID = "tx-123"
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["payment_refund_failed"] != "true" {
		t.Errorf("metadata payment_refund_failed esperado true, obtido %v", meta)
	}
}

func TestHandleResult_RetryExceededOnRefund_FailsWithRefundMetadata(t *testing.T) {
	o, pub := newTestOrchestrator(2)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentRefundPending, RetryCount: 2, TransactionID: "tx-123"})

	event := resultEvent("order-001", domain.EventPaymentCompensateResult, domain.StatusRetrying)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["payment_refund_failed"] != "true" {
		t.Errorf("metadata payment_refund_failed esperado true, obtido %v", meta)
	}
}

func TestHandleResult_Notified_CompletesOrder(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusInventoryReserved})

	event := resultEvent("order-001", domain.EventNotificationResult, domain.StatusNotified)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := currentState(t, o, "order-001")
	if state.Current != domain.StatusCompleted {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusCompleted, state.Current)
	}
	if !hasEventType(pub.events, domain.EventOrderCompleted) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderCompleted, pub.events)
	}
}

func TestHandleResult_UnexpectedStatus_ReturnsError(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPending)
	if err := o.HandleResult(context.Background(), event); err == nil {
		t.Fatal("esperado erro para status inesperado")
	}
}

// --- nextCommand --------------------------------------------------------------

func TestNextCommand_Pending(t *testing.T) {
	target, eventType, err := nextCommand(domain.StatusPending)
	if err != nil {
		t.Fatalf("nextCommand retornou erro: %v", err)
	}
	if target != domain.StatusPaymentPending || eventType != domain.EventPaymentCommand {
		t.Errorf("esperado (%s, %s), obtido (%s, %s)", domain.StatusPaymentPending, domain.EventPaymentCommand, target, eventType)
	}
}

func TestNextCommand_PaymentPending(t *testing.T) {
	target, eventType, err := nextCommand(domain.StatusPaymentPending)
	if err != nil {
		t.Fatalf("nextCommand retornou erro: %v", err)
	}
	if target != domain.StatusPaymentPending || eventType != domain.EventPaymentCommand {
		t.Errorf("esperado (%s, %s), obtido (%s, %s)", domain.StatusPaymentPending, domain.EventPaymentCommand, target, eventType)
	}
}

func TestNextCommand_PaymentApproved(t *testing.T) {
	target, eventType, err := nextCommand(domain.StatusPaymentApproved)
	if err != nil {
		t.Fatalf("nextCommand retornou erro: %v", err)
	}
	if target != domain.StatusPaymentApproved || eventType != domain.EventInventoryCommand {
		t.Errorf("esperado (%s, %s), obtido (%s, %s)", domain.StatusPaymentApproved, domain.EventInventoryCommand, target, eventType)
	}
}

func TestNextCommand_RefundPending(t *testing.T) {
	target, eventType, err := nextCommand(domain.StatusPaymentRefundPending)
	if err != nil {
		t.Fatalf("nextCommand retornou erro: %v", err)
	}
	if target != domain.StatusPaymentRefundPending || eventType != domain.EventPaymentCompensate {
		t.Errorf("esperado (%s, %s), obtido (%s, %s)", domain.StatusPaymentRefundPending, domain.EventPaymentCompensate, target, eventType)
	}
}

func TestNextCommand_InventoryReserved(t *testing.T) {
	target, eventType, err := nextCommand(domain.StatusInventoryReserved)
	if err != nil {
		t.Fatalf("nextCommand retornou erro: %v", err)
	}
	if target != domain.StatusInventoryReserved || eventType != domain.EventNotificationCommand {
		t.Errorf("esperado (%s, %s), obtido (%s, %s)", domain.StatusInventoryReserved, domain.EventNotificationCommand, target, eventType)
	}
}

func TestNextCommand_Unknown(t *testing.T) {
	if _, _, err := nextCommand(domain.StatusCompleted); err == nil {
		t.Fatal("esperado erro para status sem próxima etapa")
	}
}

// --- expectedStatusForResult -------------------------------------------------

func TestExpectedStatusForResult_Payment(t *testing.T) {
	status, err := expectedStatusForResult(domain.EventPaymentResult)
	if err != nil {
		t.Fatalf("expectedStatusForResult retornou erro: %v", err)
	}
	if status != domain.StatusPaymentPending {
		t.Errorf("esperado %s, obtido %s", domain.StatusPaymentPending, status)
	}
}

func TestExpectedStatusForResult_UnsupportedType(t *testing.T) {
	if _, err := expectedStatusForResult(domain.EventOrderCreated); err == nil {
		t.Fatal("esperado erro para tipo de evento não suportado")
	}
}

// --- validateResultStatus ----------------------------------------------------

func TestValidateResultStatus_ValidStatuses(t *testing.T) {
	cases := []struct {
		eventType domain.EventType
		status    domain.OrderStatus
	}{
		{domain.EventPaymentResult, domain.StatusPaymentApproved},
		{domain.EventPaymentResult, domain.StatusRetrying},
		{domain.EventPaymentResult, domain.StatusFailed},
		{domain.EventPaymentCompensateResult, domain.StatusPaymentRefunded},
		{domain.EventPaymentCompensateResult, domain.StatusRetrying},
		{domain.EventPaymentCompensateResult, domain.StatusFailed},
		{domain.EventInventoryResult, domain.StatusInventoryReserved},
		{domain.EventNotificationResult, domain.StatusNotified},
	}
	for _, c := range cases {
		if err := validateResultStatus(c.eventType, c.status); err != nil {
			t.Errorf("esperado status válido para (%s, %s): %v", c.eventType, c.status, err)
		}
	}
}

func TestValidateResultStatus_InvalidStatus(t *testing.T) {
	if err := validateResultStatus(domain.EventPaymentResult, domain.StatusNotified); err == nil {
		t.Fatal("esperado erro para status incompatível")
	}
}

// --- cloneMetadata e terminalEvent --------------------------------------------

func TestCloneMetadata_CopiesValues(t *testing.T) {
	original := map[string]string{"a": "1"}
	cloned := cloneMetadata(original)
	cloned["b"] = "2"

	if original["b"] != "" {
		t.Error("cloneMetadata deve devolver um mapa independente")
	}
}

func TestTerminalEvent_BuildsEvent(t *testing.T) {
	evt := terminalEvent("order-001", domain.StatusNotified, domain.StatusCompleted, domain.EventOrderCompleted, map[string]string{"k": "v"})
	if evt.OrderID != "order-001" || evt.EventType != domain.EventOrderCompleted {
		t.Errorf("evento terminal malformado: %+v", evt)
	}
	if evt.Metadata["k"] != "v" {
		t.Errorf("metadata não propagada: %+v", evt)
	}
}

// --- fluxo completo ---------------------------------------------------------------

func TestFullFlow_Success(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	orderID := "order-001"

	steps := []domain.Event{
		resultEvent(orderID, domain.EventPaymentResult, domain.StatusPaymentApproved),
		resultEvent(orderID, domain.EventInventoryResult, domain.StatusInventoryReserved),
		resultEvent(orderID, domain.EventNotificationResult, domain.StatusNotified),
	}

	if err := o.StartOrder(context.Background(), resultEvent(orderID, domain.EventOrderCreated, domain.StatusPending)); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}
	for _, step := range steps {
		step.SagaID = orderID
		if err := o.HandleResult(context.Background(), step); err != nil {
			t.Fatalf("etapa %s falhou: %v", step.EventType, err)
		}
	}

	state := currentState(t, o, orderID)
	if state.Current != domain.StatusCompleted {
		t.Errorf("estado final esperado %s, obtido %s", domain.StatusCompleted, state.Current)
	}
	if !hasEventType(pub.events, domain.EventOrderCompleted) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderCompleted, pub.events)
	}
}

func TestFullFlow_InventoryFail_TriggersCompensationThenRefund(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	orderID := "order-001"

	paymentResult := resultEvent(orderID, domain.EventPaymentResult, domain.StatusPaymentApproved)
	paymentResult.TransactionID = "tx-123"
	if err := o.StartOrder(context.Background(), resultEvent(orderID, domain.EventOrderCreated, domain.StatusPending)); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}
	if err := o.HandleResult(context.Background(), paymentResult); err != nil {
		t.Fatalf("pagamento aprovado falhou: %v", err)
	}

	inventoryFail := resultEvent(orderID, domain.EventInventoryResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), inventoryFail); err != nil {
		t.Fatalf("falha de inventário deveria acionar compensação, erro: %v", err)
	}

	state := currentState(t, o, orderID)
	if state.Current != domain.StatusPaymentRefundPending {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentRefundPending, state.Current)
	}
	if !hasEventType(pub.events, domain.EventPaymentCompensate) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventPaymentCompensate, pub.events)
	}

	refunded := resultEvent(orderID, domain.EventPaymentCompensateResult, domain.StatusPaymentRefunded)
	refunded.TransactionID = "tx-123"
	if err := o.HandleResult(context.Background(), refunded); err != nil {
		t.Fatalf("estorno concluído falhou: %v", err)
	}

	state = currentState(t, o, orderID)
	if state.Current != domain.StatusFailed {
		t.Errorf("estado final esperado %s, obtido %s", domain.StatusFailed, state.Current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
}

// --- casos de erro no publish ----------------------------------------------------

func TestComplete_PublishFails_ReturnsError(t *testing.T) {
	pub := &mockFailingPublisher{}
	o := &Orchestrator{publisher: pub, sagas: newInMemorySagaRepo(), eventLog: &fakeEventLog{}}
	saga := domain.Saga{OrderID: "order-001", Current: domain.StatusInventoryReserved}

	if err := o.complete(context.Background(), &saga, nil); err == nil {
		t.Fatal("esperado erro quando o publish do ORDER_COMPLETED falha")
	}
}

func TestFail_PublishFails_ReturnsError(t *testing.T) {
	pub := &mockFailingPublisher{}
	o := &Orchestrator{publisher: pub, sagas: newInMemorySagaRepo(), eventLog: &fakeEventLog{}}
	saga := domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending}

	if err := o.fail(context.Background(), &saga, nil); err == nil {
		t.Fatal("esperado erro quando o publish do ORDER_FAILED falha")
	}
}

// --- casos de erro no repositório e no journal -------------------------------------

func TestStartOrder_RepoLoadError_ReturnsError(t *testing.T) {
	o := &Orchestrator{publisher: &mockPublisher{}, sagas: &mockFailingSagaRepo{}, eventLog: &fakeEventLog{}}

	if err := o.StartOrder(context.Background(), resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)); err == nil {
		t.Fatal("esperado erro quando o load da saga falha")
	}
}

func TestHandleResult_RepoLoadError_ReturnsError(t *testing.T) {
	o := &Orchestrator{publisher: &mockPublisher{}, sagas: &mockFailingSagaRepo{}, eventLog: &fakeEventLog{}}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	if err := o.HandleResult(context.Background(), event); err == nil {
		t.Fatal("esperado erro quando o load da saga falha")
	}
}

func TestStartOrder_EventLogFailure_ReturnsError(t *testing.T) {
	o := &Orchestrator{publisher: &mockPublisher{}, sagas: newInMemorySagaRepo(), eventLog: &mockFailingEventLog{}}

	if err := o.StartOrder(context.Background(), resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)); err == nil {
		t.Fatal("esperado erro quando a gravação do journal falha")
	}
}

func TestHandleResult_EventLogFailure_ReturnsError(t *testing.T) {
	o := &Orchestrator{publisher: &mockPublisher{}, sagas: newInMemorySagaRepo(), eventLog: &mockFailingEventLog{}}
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	if err := o.HandleResult(context.Background(), event); err == nil {
		t.Fatal("esperado erro quando a gravação do journal falha no HandleResult")
	}
}

func TestStartOrder_DispatchNextUnknownState(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	saga := domain.Saga{OrderID: "order-001", Current: domain.StatusCompleted}

	if err := o.dispatchNext(context.Background(), &saga); err == nil {
		t.Fatal("esperado erro quando o estado não tem próxima etapa")
	}
	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

// --- journal de eventos (rastreabilidade) -----------------------------------------

func TestStartOrder_LogsOrderCreatedIn(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	log := o.eventLog.(*fakeEventLog)

	event := resultEvent("order-001", domain.EventOrderCreated, domain.StatusPending)
	if err := o.StartOrder(context.Background(), event); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}

	if len(log.entries) < 2 {
		t.Fatalf("esperado ao menos 2 registros no journal, obtidos %d", len(log.entries))
	}
	in := log.entries[0]
	if in.Direction != application.DirectionIn || in.EventType != domain.EventOrderCreated || in.Component != "orchestrator" {
		t.Errorf("primeiro registro deveria ser IN do ORDER_CREATED: %+v", in)
	}
	out := log.entries[1]
	if out.Direction != application.DirectionOut || out.EventType != domain.EventPaymentCommand {
		t.Errorf("segundo registro deveria ser OUT do PAYMENT_COMMAND: %+v", out)
	}
}

func TestHandleResult_LogsResultInAndCommandOut(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	log := o.eventLog.(*fakeEventLog)
	startOrder(t, o, "order-001")

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	event.TransactionID = "tx-123"
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult falhou: %v", err)
	}

	var foundIn, foundOut bool
	for _, entry := range log.entries {
		if entry.Direction == application.DirectionIn && entry.EventType == domain.EventPaymentResult {
			foundIn = true
		}
		if entry.Direction == application.DirectionOut && entry.EventType == domain.EventInventoryCommand {
			foundOut = true
		}
	}
	if !foundIn {
		t.Error("journal deveria registrar IN do PAYMENT_RESULT")
	}
	if !foundOut {
		t.Error("journal deveria registrar OUT do INVENTORY_COMMAND")
	}
}

func TestFail_LogsTerminalOut(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	log := o.eventLog.(*fakeEventLog)
	seedState(t, o, domain.Saga{OrderID: "order-001", Current: domain.StatusPaymentPending})

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), event); err != nil {
		t.Fatalf("HandleResult falhou: %v", err)
	}

	var found bool
	for _, entry := range log.entries {
		if entry.Direction == application.DirectionOut && entry.EventType == domain.EventOrderFailed {
			found = true
		}
	}
	if !found {
		t.Error("journal deveria registrar OUT do ORDER_FAILED")
	}
}
