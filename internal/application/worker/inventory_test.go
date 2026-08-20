package worker

import (
	"context"
	"testing"

	"workers-kafka/internal/domain"
)

// --- InventoryUseCase ----------------------------------------------------------

func TestInventoryUseCase_Handle_IgnoredEventType(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewInventoryUseCase(&mockInventoryGateway{}, pub)

	err := uc.Handle(context.Background(), resultEvent("order-001", domain.EventInventoryResult, domain.StatusInventoryReserved))
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}
	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

func TestInventoryUseCase_Handle_InvalidStatus(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewInventoryUseCase(&mockInventoryGateway{}, pub)

	event := resultEvent("order-001", domain.EventInventoryCommand, domain.StatusPending)
	err := uc.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para status inválido")
	}
}

func TestInventoryUseCase_Handle_Reserved(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockInventoryGateway{reserved: true}
	uc := NewInventoryUseCase(gateway, pub)

	event := resultEvent("order-001", domain.EventInventoryCommand, domain.StatusPaymentApproved)
	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	result := pub.events[0]
	if result.EventType != domain.EventInventoryResult {
		t.Errorf("tipo esperado %s, obtido %s", domain.EventInventoryResult, result.EventType)
	}
	if result.StatusAtual != domain.StatusInventoryReserved {
		t.Errorf("status esperado %s, obtido %s", domain.StatusInventoryReserved, result.StatusAtual)
	}
	if result.StatusAnterior != domain.StatusPaymentApproved {
		t.Errorf("status anterior esperado %s, obtido %s", domain.StatusPaymentApproved, result.StatusAnterior)
	}
}

func TestInventoryUseCase_Handle_NotReserved(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockInventoryGateway{reserved: false}
	uc := NewInventoryUseCase(gateway, pub)

	event := resultEvent("order-001", domain.EventInventoryCommand, domain.StatusPaymentApproved)
	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	if pub.events[0].StatusAtual != domain.StatusFailed {
		t.Errorf("status esperado %s, obtido %s", domain.StatusFailed, pub.events[0].StatusAtual)
	}
}

func TestInventoryUseCase_Handle_GatewayError(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockInventoryGateway{err: errSimulado}
	uc := NewInventoryUseCase(gateway, pub)

	event := resultEvent("order-001", domain.EventInventoryCommand, domain.StatusPaymentApproved)
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
