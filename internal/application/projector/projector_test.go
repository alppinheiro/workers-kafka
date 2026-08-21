package projector

import (
	"context"
	"errors"
	"testing"

	"workers-kafka/internal/domain"
)

// fakeViews implementa application.OrderViewRepository para os testes do projector.
type fakeViews struct {
	applied   []domain.Event
	processed map[string]bool
	failMark  bool
	failApply bool
}

func newFakeViews() *fakeViews {
	return &fakeViews{processed: make(map[string]bool)}
}

func (f *fakeViews) ApplyEvent(_ context.Context, event domain.Event) error {
	if f.failApply {
		return errors.New("apply falhou")
	}
	f.applied = append(f.applied, event)
	return nil
}

func (f *fakeViews) MarkProcessed(_ context.Context, eventID string) (bool, error) {
	if f.failMark {
		return false, errors.New("mark falhou")
	}
	if f.processed[eventID] {
		return false, nil
	}
	f.processed[eventID] = true
	return true, nil
}

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

func TestHandleEvent_AppliesNewEvent(t *testing.T) {
	views := newFakeViews()
	p := New(views)

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	if err := p.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("HandleEvent retornou erro inesperado: %v", err)
	}

	if len(views.applied) != 1 {
		t.Fatalf("esperado 1 evento aplicado, obtidos %d", len(views.applied))
	}
	if views.applied[0].EventID != event.EventID {
		t.Errorf("evento aplicado incorreto: %+v", views.applied[0])
	}
}

func TestHandleEvent_SkipsAlreadyProcessed(t *testing.T) {
	views := newFakeViews()
	p := New(views)

	event := resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	if err := p.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("primeira chamada falhou: %v", err)
	}
	if err := p.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("segunda chamada falhou: %v", err)
	}

	if len(views.applied) != 1 {
		t.Errorf("evento duplicado não deveria ser aplicado novamente; aplicados=%d", len(views.applied))
	}
}

func TestHandleEvent_MarkProcessedError_ReturnsError(t *testing.T) {
	views := newFakeViews()
	views.failMark = true
	p := New(views)

	if err := p.HandleEvent(context.Background(), resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)); err == nil {
		t.Fatal("esperado erro quando MarkProcessed falha")
	}
}

func TestHandleEvent_ApplyError_ReturnsError(t *testing.T) {
	views := newFakeViews()
	views.failApply = true
	p := New(views)

	if err := p.HandleEvent(context.Background(), resultEvent("order-001", domain.EventPaymentResult, domain.StatusPaymentApproved)); err == nil {
		t.Fatal("esperado erro quando ApplyEvent falha")
	}
}
