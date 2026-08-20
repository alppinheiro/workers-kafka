package orchestrator

import (
	"context"
	"testing"

	"workers-kafka/internal/domain"
)

// --- helpers de teste -------------------------------------------------------

// newTestOrchestrator cria um orquestrador com o mock publisher e limite de retries configurável.
func newTestOrchestrator(maxRetries int) (*Orchestrator, *mockPublisher) {
	pub := &mockPublisher{}
	return New(pub, maxRetries), pub
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

// --- StartOrder --------------------------------------------------------------

func TestStartOrder_Success(t *testing.T) {
	o, pub := newTestOrchestrator(3)

	err := o.StartOrder(context.Background(), "order-001")
	if err != nil {
		t.Fatalf("StartOrder retornou erro inesperado: %v", err)
	}

	state, ok := o.states["order-001"]
	if !ok {
		t.Fatal("estado da saga não foi criado")
	}
	if state.current != domain.StatusPaymentPending {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentPending, state.current)
	}
	if state.previous != domain.StatusPending {
		t.Errorf("estado anterior esperado %s, obtido %s", domain.StatusPending, state.previous)
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

	if err := o.StartOrder(context.Background(), "order-001"); err != nil {
		t.Fatalf("primeira chamada falhou: %v", err)
	}

	err := o.StartOrder(context.Background(), "order-001")
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

	if _, ok := o.states["order-001"]; !ok {
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

// --- HandleResult: avanço normal ---------------------------------------------

func TestHandleResult_PaymentApproved_AdvancesAndDispatchesInventory(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	if err := o.StartOrder(context.Background(), "order-001"); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	event.TransactionID = "tx-123"
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusPaymentApproved {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentApproved, state.current)
	}
	if state.transactionID != "tx-123" {
		t.Errorf("transactionID esperado tx-123, obtido %s", state.transactionID)
	}
	if !hasEventType(pub.events, domain.EventInventoryCommand) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventInventoryCommand, pub.events)
	}
}

func TestHandleResult_InventoryReserved_AdvancesAndDispatchesNotification(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentApproved, transactionID: "tx-123"}

	event := resultEvent("order-001", domain.EventInventoryResult, domain.StatusInventoryReserved)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusInventoryReserved {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusInventoryReserved, state.current)
	}
	if !hasEventType(pub.events, domain.EventNotificationCommand) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventNotificationCommand, pub.events)
	}
}

// --- HandleResult: retry -----------------------------------------------------

func TestHandleResult_Retrying_RedispatchesSameCommand(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentPending}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusRetrying)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.retryCount != 1 {
		t.Errorf("retryCount esperado 1, obtido %d", state.retryCount)
	}
	if !hasEventType(pub.events, domain.EventPaymentCommand) {
		t.Errorf("esperado novo comando %s, obtidos %v", domain.EventPaymentCommand, pub.events)
	}
}

func TestHandleResult_RetryExceededOnPayment_FailsOrder(t *testing.T) {
	o, pub := newTestOrchestrator(2)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentPending, retryCount: 2}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusRetrying)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.current)
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
	o.states["order-001"] = &sagaState{current: domain.StatusInventoryReserved, retryCount: 2}

	event := resultEvent("order-001", domain.EventNotificationResult, domain.StatusRetrying)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusCompleted {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusCompleted, state.current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["notification_error"] != "true" {
		t.Errorf("metadata notification_error esperado true, obtido %v", meta)
	}
}

// --- HandleResult: falha e compensação ---------------------------------------

func TestHandleResult_InventoryFailed_TriggersCompensation(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentApproved, transactionID: "tx-123"}

	event := resultEvent("order-001", domain.EventInventoryResult, domain.StatusFailed)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusPaymentRefundPending {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentRefundPending, state.current)
	}
	if !hasEventType(pub.events, domain.EventPaymentCompensate) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventPaymentCompensate, pub.events)
	}
}

func TestHandleResult_PaymentFailed_FailsOrder(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentPending}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusFailed)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
}

func TestHandleResult_NotificationFailed_StillCompletes(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusInventoryReserved}

	event := resultEvent("order-001", domain.EventNotificationResult, domain.StatusFailed)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusCompleted {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusCompleted, state.current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["notification_error"] != "true" {
		t.Errorf("metadata notification_error esperado true, obtido %v", meta)
	}
}

// --- HandleResult: erros de validação ----------------------------------------

func TestHandleResult_UnknownSaga(t *testing.T) {
	o, _ := newTestOrchestrator(3)

	event := resultEvent("order-desconhecida", domain.EventPaymentResult, domain.StatusPaymentApproved)
	err := o.HandleResult(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para saga desconhecida")
	}
}

func TestHandleResult_OutOfOrderEvent(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusInventoryReserved}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	err := o.HandleResult(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para evento fora de ordem")
	}
}

func TestHandleResult_InvalidResultStatus(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentPending}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusCompleted)
	err := o.HandleResult(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para status inválido no resultado")
	}
}

func TestHandleResult_Refunded_FailsWithRefundMetadata(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentRefundPending, transactionID: "tx-123"}

	event := resultEvent("order-001", domain.EventPaymentCompensateResult, domain.StatusPaymentRefunded)
	event.TransactionID = "tx-123"
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["payment_refunded"] != "true" {
		t.Errorf("metadata payment_refunded esperado true, obtido %v", meta)
	}
}

func TestHandleResult_RefundFailed_FailsWithRefundFailedMetadata(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentRefundPending, transactionID: "tx-123"}

	event := resultEvent("order-001", domain.EventPaymentCompensateResult, domain.StatusFailed)
	event.TransactionID = "tx-123"
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["payment_refund_failed"] != "true" {
		t.Errorf("metadata payment_refund_failed esperado true, obtido %v", meta)
	}
}

func TestHandleResult_RetryExceededOnRefund_FailsWithRefundMetadata(t *testing.T) {
	o, pub := newTestOrchestrator(2)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentRefundPending, retryCount: 2, transactionID: "tx-123"}

	event := resultEvent("order-001", domain.EventPaymentCompensateResult, domain.StatusRetrying)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.current)
	}
	meta := lastEvent(pub.events).Metadata
	if meta["payment_refund_failed"] != "true" {
		t.Errorf("metadata payment_refund_failed esperado true, obtido %v", meta)
	}
}

func TestHandleResult_Notified_CompletesOrder(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusInventoryReserved}

	event := resultEvent("order-001", domain.EventNotificationResult, domain.StatusNotified)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusCompleted {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusCompleted, state.current)
	}
	if !hasEventType(pub.events, domain.EventOrderCompleted) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderCompleted, pub.events)
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

func TestNextCommand_PaymentRefundPending(t *testing.T) {
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

func TestNextCommand_UnknownStatus(t *testing.T) {
	_, _, err := nextCommand(domain.StatusCompleted)
	if err == nil {
		t.Fatal("esperado erro para status sem próxima etapa")
	}
}

// --- expectedStatusForResult ---------------------------------------------------

func TestExpectedStatusForResult_PaymentResult(t *testing.T) {
	expected, err := expectedStatusForResult(domain.EventPaymentResult)
	if err != nil {
		t.Fatalf("expectedStatusForResult retornou erro: %v", err)
	}
	if expected != domain.StatusPaymentPending {
		t.Errorf("esperado %s, obtido %s", domain.StatusPaymentPending, expected)
	}
}

// --- validateResultStatus -----------------------------------------------------

func TestValidateResultStatus_PaymentResult_Valid(t *testing.T) {
	valid := []domain.OrderStatus{domain.StatusPaymentApproved, domain.StatusRetrying, domain.StatusFailed}
	for _, status := range valid {
		if err := validateResultStatus(domain.EventPaymentResult, status); err != nil {
			t.Errorf("status %s deveria ser válido: %v", status, err)
		}
	}
}

func TestValidateResultStatus_PaymentResult_Invalid(t *testing.T) {
	invalid := []domain.OrderStatus{domain.StatusPaymentRefunded, domain.StatusInventoryReserved, domain.StatusNotified, domain.StatusCompleted}
	for _, status := range invalid {
		if err := validateResultStatus(domain.EventPaymentResult, status); err == nil {
			t.Errorf("status %s deveria ser inválido", status)
		}
	}
}

func TestValidateResultStatus_InventoryResult_Valid(t *testing.T) {
	valid := []domain.OrderStatus{domain.StatusInventoryReserved, domain.StatusRetrying, domain.StatusFailed}
	for _, status := range valid {
		if err := validateResultStatus(domain.EventInventoryResult, status); err != nil {
			t.Errorf("status %s deveria ser válido: %v", status, err)
		}
	}
}

func TestValidateResultStatus_InventoryResult_Invalid(t *testing.T) {
	if err := validateResultStatus(domain.EventInventoryResult, domain.StatusPaymentApproved); err == nil {
		t.Error("status PAYMENT_APPROVED deveria ser inválido para INVENTORY_RESULT")
	}
}

func TestValidateResultStatus_NotificationResult_Valid(t *testing.T) {
	valid := []domain.OrderStatus{domain.StatusNotified, domain.StatusRetrying, domain.StatusFailed}
	for _, status := range valid {
		if err := validateResultStatus(domain.EventNotificationResult, status); err != nil {
			t.Errorf("status %s deveria ser válido: %v", status, err)
		}
	}
}

func TestValidateResultStatus_CompensateResult_Valid(t *testing.T) {
	valid := []domain.OrderStatus{domain.StatusPaymentRefunded, domain.StatusRetrying, domain.StatusFailed}
	for _, status := range valid {
		if err := validateResultStatus(domain.EventPaymentCompensateResult, status); err != nil {
			t.Errorf("status %s deveria ser válido: %v", status, err)
		}
	}
}

// --- cloneMetadata --------------------------------------------------------------

func TestCloneMetadata_Nil(t *testing.T) {
	cloned := cloneMetadata(nil)
	if cloned == nil || len(cloned) != 0 {
		t.Errorf("esperado mapa vazio para nil, obtido %v", cloned)
	}
}

func TestCloneMetadata_CopiesValues(t *testing.T) {
	original := map[string]string{"key1": "value1", "key2": "value2"}
	cloned := cloneMetadata(original)

	if len(cloned) != len(original) {
		t.Errorf("esperado %d itens, obtido %d", len(original), len(cloned))
	}
	if cloned["key1"] != "value1" || cloned["key2"] != "value2" {
		t.Errorf("conteúdo do clone incorreto: %v", cloned)
	}

	// garantir que o clone não afeta o original
	cloned["nova-chave"] = "valor"
	if _, exists := original["nova-chave"]; exists {
		t.Error("alterar o clone não deveria afetar o original")
	}
}

// --- terminalEvent ---------------------------------------------------------------

func TestTerminalEvent_Fields(t *testing.T) {
	meta := map[string]string{"motivo": "teste"}
	event := terminalEvent("order-001", domain.StatusPending, domain.StatusFailed, domain.EventOrderFailed, meta)

	if event.OrderID != "order-001" || event.SagaID != "order-001" {
		t.Errorf("IDs incorretos: %+v", event)
	}
	if event.StatusAnterior != domain.StatusPending || event.StatusAtual != domain.StatusFailed {
		t.Errorf("status incorretos: %+v", event)
	}
	if event.EventType != domain.EventOrderFailed {
		t.Errorf("tipo de evento incorreto: %s", event.EventType)
	}
	if event.SchemaVersion != domain.CurrentSchemaVersion {
		t.Errorf("schema version incorreto: %d", event.SchemaVersion)
	}
	if event.EventID == "" {
		t.Error("EventID não deveria ser vazio")
	}
	if event.CreatedAt.IsZero() {
		t.Error("CreatedAt não deveria ser zero")
	}
	if event.Metadata["motivo"] != "teste" {
		t.Errorf("metadata incorreta: %v", event.Metadata)
	}
}

func TestValidateResultStatus_UnknownType(t *testing.T) {
	if err := validateResultStatus(domain.EventOrderCreated, domain.StatusPending); err == nil {
		t.Error("tipo de evento desconhecido deveria retornar erro")
	}
}

func TestExpectedStatusForResult_PaymentCompensateResult(t *testing.T) {
	expected, err := expectedStatusForResult(domain.EventPaymentCompensateResult)
	if err != nil {
		t.Fatalf("expectedStatusForResult retornou erro: %v", err)
	}
	if expected != domain.StatusPaymentRefundPending {
		t.Errorf("esperado %s, obtido %s", domain.StatusPaymentRefundPending, expected)
	}
}

func TestExpectedStatusForResult_InventoryResult(t *testing.T) {
	expected, err := expectedStatusForResult(domain.EventInventoryResult)
	if err != nil {
		t.Fatalf("expectedStatusForResult retornou erro: %v", err)
	}
	if expected != domain.StatusPaymentApproved {
		t.Errorf("esperado %s, obtido %s", domain.StatusPaymentApproved, expected)
	}
}

func TestExpectedStatusForResult_NotificationResult(t *testing.T) {
	expected, err := expectedStatusForResult(domain.EventNotificationResult)
	if err != nil {
		t.Fatalf("expectedStatusForResult retornou erro: %v", err)
	}
	if expected != domain.StatusInventoryReserved {
		t.Errorf("esperado %s, obtido %s", domain.StatusInventoryReserved, expected)
	}
}

func TestExpectedStatusForResult_UnknownType(t *testing.T) {
	_, err := expectedStatusForResult(domain.EventOrderCreated)
	if err == nil {
		t.Fatal("esperado erro para tipo de evento não suportado")
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

	if err := o.StartOrder(context.Background(), orderID); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}
	for _, step := range steps {
		step.SagaID = orderID
		if err := o.HandleResult(context.Background(), step); err != nil {
			t.Fatalf("etapa %s falhou: %v", step.EventType, err)
		}
	}

	state := o.states[orderID]
	if state.current != domain.StatusCompleted {
		t.Errorf("estado final esperado %s, obtido %s", domain.StatusCompleted, state.current)
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
	if err := o.StartOrder(context.Background(), orderID); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}
	if err := o.HandleResult(context.Background(), paymentResult); err != nil {
		t.Fatalf("pagamento aprovado falhou: %v", err)
	}

	inventoryFail := resultEvent(orderID, domain.EventInventoryResult, domain.StatusFailed)
	if err := o.HandleResult(context.Background(), inventoryFail); err != nil {
		t.Fatalf("falha de inventário deveria acionar compensação, erro: %v", err)
	}

	state := o.states[orderID]
	if state.current != domain.StatusPaymentRefundPending {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusPaymentRefundPending, state.current)
	}
	if !hasEventType(pub.events, domain.EventPaymentCompensate) {
		t.Errorf("esperado comando %s, obtidos %v", domain.EventPaymentCompensate, pub.events)
	}

	refunded := resultEvent(orderID, domain.EventPaymentCompensateResult, domain.StatusPaymentRefunded)
	refunded.TransactionID = "tx-123"
	if err := o.HandleResult(context.Background(), refunded); err != nil {
		t.Fatalf("estorno concluído falhou: %v", err)
	}

	if state.current != domain.StatusFailed {
		t.Errorf("estado final esperado %s, obtido %s", domain.StatusFailed, state.current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
}

// --- casos de erro no publish ----------------------------------------------------

func TestComplete_PublishFails_ReturnsError(t *testing.T) {
	pub := &mockFailingPublisher{}
	o := &Orchestrator{publisher: pub, states: make(map[string]*sagaState)}
	o.states["order-001"] = &sagaState{current: domain.StatusInventoryReserved}

	err := o.complete(context.Background(), "order-001", o.states["order-001"], nil)
	if err == nil {
		t.Fatal("esperado erro quando o publish do ORDER_COMPLETED falha")
	}
}

func TestFail_PublishFails_ReturnsError(t *testing.T) {
	pub := &mockFailingPublisher{}
	o := &Orchestrator{publisher: pub, states: make(map[string]*sagaState)}
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentPending}

	err := o.fail(context.Background(), "order-001", o.states["order-001"], nil)
	if err == nil {
		t.Fatal("esperado erro quando o publish do ORDER_FAILED falha")
	}
}

func TestStartOrder_DispatchNextUnknownState(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusCompleted}

	err := o.dispatchNext(context.Background(), "order-001")
	if err == nil {
		t.Fatal("esperado erro quando o estado não tem próxima etapa")
	}
	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

func TestDispatchNext_UnknownSaga(t *testing.T) {
	o, _ := newTestOrchestrator(3)

	err := o.dispatchNext(context.Background(), "order-desconhecida")
	if err == nil {
		t.Fatal("esperado erro para saga desconhecida")
	}
}

func TestHandleResult_InventoryFailedWithoutTransactionID_FailsOrder(t *testing.T) {
	o, pub := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentApproved} // sem transactionID

	event := resultEvent("order-001", domain.EventInventoryResult, domain.StatusFailed)
	err := o.HandleResult(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleResult retornou erro inesperado: %v", err)
	}

	state := o.states["order-001"]
	if state.current != domain.StatusFailed {
		t.Errorf("estado esperado %s, obtido %s", domain.StatusFailed, state.current)
	}
	if !hasEventType(pub.events, domain.EventOrderFailed) {
		t.Errorf("esperado evento %s, obtidos %v", domain.EventOrderFailed, pub.events)
	}
}

func TestHandleResult_UnexpectedStatus_ReturnsError(t *testing.T) {
	o, _ := newTestOrchestrator(3)
	o.states["order-001"] = &sagaState{current: domain.StatusPaymentPending}

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPending)
	err := o.HandleResult(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para status inesperado")
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
