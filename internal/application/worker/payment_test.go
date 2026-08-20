package worker

import (
	"context"
	"testing"

	"workers-kafka/internal/domain"
)

// paymentCommand cria um comando de pagamento com valores padrão.
func paymentCommand(orderID string) domain.Event {
	return domain.Event{
		EventID:       "evt-pay",
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   domain.StatusPaymentPending,
		EventType:     domain.EventPaymentCommand,
		SchemaVersion: domain.CurrentSchemaVersion,
	}
}

func TestPaymentUseCase_Handle_IgnoredEventType(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewPaymentUseCase(&mockPaymentGateway{}, pub)

	err := uc.Handle(context.Background(), resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved))
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}
	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

func TestPaymentUseCase_Handle_InvalidStatus(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewPaymentUseCase(&mockPaymentGateway{}, pub)

	event := paymentCommand("order-001")
	event.StatusAtual = domain.StatusPaymentApproved // status errado

	err := uc.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para status inválido")
	}
	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

func TestPaymentUseCase_Handle_Approved(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockPaymentGateway{approved: true, transactionID: "tx-1"}
	uc := NewPaymentUseCase(gateway, pub)

	err := uc.Handle(context.Background(), paymentCommand("order-001"))
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	if len(pub.events) != 1 {
		t.Fatalf("esperado 1 evento publicado, obtido %d", len(pub.events))
	}
	result := pub.events[0]
	if result.EventType != domain.EventPaymentResult {
		t.Errorf("tipo esperado %s, obtido %s", domain.EventPaymentResult, result.EventType)
	}
	if result.StatusAtual != domain.StatusPaymentApproved {
		t.Errorf("status esperado %s, obtido %s", domain.StatusPaymentApproved, result.StatusAtual)
	}
	if result.TransactionID != "tx-1" {
		t.Errorf("transactionID esperado tx-1, obtido %s", result.TransactionID)
	}
}

func TestPaymentUseCase_Handle_Rejected(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockPaymentGateway{approved: false}
	uc := NewPaymentUseCase(gateway, pub)

	err := uc.Handle(context.Background(), paymentCommand("order-001"))
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	result := pub.events[0]
	if result.StatusAtual != domain.StatusFailed {
		t.Errorf("status esperado %s, obtido %s", domain.StatusFailed, result.StatusAtual)
	}
}

func TestPaymentUseCase_Handle_GatewayError(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockPaymentGateway{err: errSimulado}
	uc := NewPaymentUseCase(gateway, pub)

	err := uc.Handle(context.Background(), paymentCommand("order-001"))
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	result := pub.events[0]
	if result.StatusAtual != domain.StatusRetrying {
		t.Errorf("status esperado %s, obtido %s", domain.StatusRetrying, result.StatusAtual)
	}
	if result.Metadata["erro"] == "" {
		t.Error("metadata de erro deveria conter o motivo")
	}
}

func TestPaymentUseCase_Handle_Compensate_Success(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockPaymentGateway{refunded: true}
	uc := NewPaymentUseCase(gateway, pub)

	event := paymentCommand("order-001")
	event.EventType = domain.EventPaymentCompensate
	event.StatusAtual = domain.StatusPaymentRefundPending
	event.TransactionID = "tx-1"

	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	result := pub.events[0]
	if result.EventType != domain.EventPaymentCompensateResult {
		t.Errorf("tipo esperado %s, obtido %s", domain.EventPaymentCompensateResult, result.EventType)
	}
	if result.StatusAtual != domain.StatusPaymentRefunded {
		t.Errorf("status esperado %s, obtido %s", domain.StatusPaymentRefunded, result.StatusAtual)
	}
	if result.TransactionID != "tx-1" {
		t.Errorf("transactionID esperado tx-1, obtido %s", result.TransactionID)
	}
}

func TestPaymentUseCase_Handle_Compensate_InvalidStatus(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewPaymentUseCase(&mockPaymentGateway{}, pub)

	event := paymentCommand("order-001")
	event.EventType = domain.EventPaymentCompensate
	event.StatusAtual = domain.StatusPaymentPending // status errado
	event.TransactionID = "tx-1"

	err := uc.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para status inválido no estorno")
	}
}

func TestPaymentUseCase_Handle_Compensate_MissingTransactionID(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewPaymentUseCase(&mockPaymentGateway{}, pub)

	event := paymentCommand("order-001")
	event.EventType = domain.EventPaymentCompensate
	event.StatusAtual = domain.StatusPaymentRefundPending
	event.TransactionID = "" // ausente

	err := uc.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para transaction_id ausente")
	}
}

func TestPaymentUseCase_Handle_Compensate_RefundFailed(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockPaymentGateway{refunded: false}
	uc := NewPaymentUseCase(gateway, pub)

	event := paymentCommand("order-001")
	event.EventType = domain.EventPaymentCompensate
	event.StatusAtual = domain.StatusPaymentRefundPending
	event.TransactionID = "tx-1"

	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	if pub.events[0].StatusAtual != domain.StatusFailed {
		t.Errorf("status esperado %s, obtido %s", domain.StatusFailed, pub.events[0].StatusAtual)
	}
}

func TestPaymentUseCase_Handle_Compensate_GatewayError(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockPaymentGateway{refundErr: errSimulado}
	uc := NewPaymentUseCase(gateway, pub)

	event := paymentCommand("order-001")
	event.EventType = domain.EventPaymentCompensate
	event.StatusAtual = domain.StatusPaymentRefundPending
	event.TransactionID = "tx-1"

	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	result := pub.events[0]
	if result.StatusAtual != domain.StatusRetrying {
		t.Errorf("status esperado %s, obtido %s", domain.StatusRetrying, result.StatusAtual)
	}
	if result.Metadata["erro"] == "" {
		t.Error("metadata de erro deveria conter o motivo")
	}
}
