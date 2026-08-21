package worker

import (
	"context"
	"testing"

	"workers-kafka/internal/domain"
)

// --- NotificationUseCase -------------------------------------------------------

func TestNotificationUseCase_Handle_IgnoredEventType(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewNotificationUseCase(&mockNotificationGateway{}, pub, &fakeEventLog{})

	err := uc.Handle(context.Background(), resultEvent("order-001", domain.EventNotificationResult, domain.StatusNotified))
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}
	if len(pub.events) != 0 {
		t.Errorf("nenhum evento deveria ser publicado, obtidos %v", pub.events)
	}
}

func TestNotificationUseCase_Handle_InvalidStatus(t *testing.T) {
	pub := &mockPublisher{}
	uc := NewNotificationUseCase(&mockNotificationGateway{}, pub, &fakeEventLog{})

	event := resultEvent("order-001", domain.EventNotificationCommand, domain.StatusPending)
	err := uc.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para status inválido")
	}
}

func TestNotificationUseCase_Handle_Notified(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockNotificationGateway{sent: true}
	uc := NewNotificationUseCase(gateway, pub, &fakeEventLog{})

	event := resultEvent("order-001", domain.EventNotificationCommand, domain.StatusInventoryReserved)
	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	result := pub.events[0]
	if result.EventType != domain.EventNotificationResult {
		t.Errorf("tipo esperado %s, obtido %s", domain.EventNotificationResult, result.EventType)
	}
	if result.StatusAtual != domain.StatusNotified {
		t.Errorf("status esperado %s, obtido %s", domain.StatusNotified, result.StatusAtual)
	}
	if result.StatusAnterior != domain.StatusInventoryReserved {
		t.Errorf("status anterior esperado %s, obtido %s", domain.StatusInventoryReserved, result.StatusAnterior)
	}
}

func TestNotificationUseCase_Handle_NotSent(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockNotificationGateway{sent: false}
	uc := NewNotificationUseCase(gateway, pub, &fakeEventLog{})

	event := resultEvent("order-001", domain.EventNotificationCommand, domain.StatusInventoryReserved)
	err := uc.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle retornou erro inesperado: %v", err)
	}

	if pub.events[0].StatusAtual != domain.StatusFailed {
		t.Errorf("status esperado %s, obtido %s", domain.StatusFailed, pub.events[0].StatusAtual)
	}
}

func TestNotificationUseCase_Handle_GatewayError(t *testing.T) {
	pub := &mockPublisher{}
	gateway := &mockNotificationGateway{err: errSimulado}
	uc := NewNotificationUseCase(gateway, pub, &fakeEventLog{})

	event := resultEvent("order-001", domain.EventNotificationCommand, domain.StatusInventoryReserved)
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

// --- Coordinator ---------------------------------------------------------------

func TestCoordinator_CreateOrder(t *testing.T) {
	pub := &mockPublisher{}
	c := NewCoordinator(pub)

	err := c.CreateOrder(context.Background(), "order-001")
	if err != nil {
		t.Fatalf("CreateOrder retornou erro inesperado: %v", err)
	}

	if len(pub.events) != 1 {
		t.Fatalf("esperado 1 evento publicado, obtido %d", len(pub.events))
	}
	event := pub.events[0]
	if event.EventType != domain.EventOrderCreated {
		t.Errorf("tipo esperado %s, obtido %s", domain.EventOrderCreated, event.EventType)
	}
	if event.OrderID != "order-001" || event.SagaID != "order-001" {
		t.Errorf("IDs incorretos: %+v", event)
	}
	if event.StatusAtual != domain.StatusPending {
		t.Errorf("status esperado %s, obtido %s", domain.StatusPending, event.StatusAtual)
	}
	if event.EventID == "" {
		t.Error("EventID não deveria ser vazio")
	}
	if event.CreatedAt.IsZero() {
		t.Error("CreatedAt não deveria ser zero")
	}
}
