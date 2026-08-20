package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewEventID_NotEmpty(t *testing.T) {
	id := NewEventID()
	if id == "" {
		t.Fatal("EventID não deveria ser vazio")
	}
}

func TestNewEventID_Unique(t *testing.T) {
	a := NewEventID()
	b := NewEventID()
	if a == b {
		t.Error("IDs gerados não deveriam colidir")
	}
}

func TestOrder_Defaults(t *testing.T) {
	order := Order{}
	if order.ID != "" {
		t.Errorf("ID esperado vazio, obtido %q", order.ID)
	}
	if order.Status != "" {
		t.Errorf("Status esperado vazio, obtido %q", order.Status)
	}
}

func TestEventType_Values(t *testing.T) {
	if string(EventOrderCreated) != "ORDER_CREATED" {
		t.Errorf("EventOrderCreated inesperado: %s", EventOrderCreated)
	}
	if string(EventPaymentResult) != "PAYMENT_RESULT" {
		t.Errorf("EventPaymentResult inesperado: %s", EventPaymentResult)
	}
	if string(EventOrderCompleted) != "ORDER_COMPLETED" {
		t.Errorf("EventOrderCompleted inesperado: %s", EventOrderCompleted)
	}
}

func TestOrderStatus_Values(t *testing.T) {
	if string(StatusPending) != "PENDING" {
		t.Errorf("StatusPending inesperado: %s", StatusPending)
	}
	if string(StatusCompleted) != "COMPLETED" {
		t.Errorf("StatusCompleted inesperado: %s", StatusCompleted)
	}
	if string(StatusFailed) != "FAILED" {
		t.Errorf("StatusFailed inesperado: %s", StatusFailed)
	}
}

func TestEvent_JSONTags(t *testing.T) {
	event := Event{EventID: "e", OrderID: "o", StatusAtual: StatusPending}
	json, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal falhou: %v", err)
	}
	raw := string(json)
	for _, tag := range []string{"event_id", "order_id", "status_atual"} {
		if !strings.Contains(raw, tag) {
			t.Errorf("tag JSON %q ausente em %s", tag, raw)
		}
	}
}
