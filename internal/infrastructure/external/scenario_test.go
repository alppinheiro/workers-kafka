package external

import (
	"context"
	"strings"
	"testing"
)

// --- matchScenario --------------------------------------------------------------

func TestMatchScenario(t *testing.T) {
	cases := []struct {
		name     string
		orderID  string
		stage    string
		expected scenarioMode
	}{
		{"fail", "order-payment-fail-001", "payment", scenarioFail},
		{"retry-once", "order-inventory-retry-once-001", "inventory", scenarioRetryOnce},
		{"retry", "order-notification-retry-001", "notification", scenarioRetry},
		{"none", "order-001", "payment", scenarioNone},
		{"retry-once prioritized before retry", "order-inventory-retry-once-001", "inventory", scenarioRetryOnce},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchScenario(tc.orderID, tc.stage); got != tc.expected {
				t.Errorf("matchScenario(%q, %q) = %q, esperado %q", tc.orderID, tc.stage, got, tc.expected)
			}
		})
	}
}

func TestRetryError(t *testing.T) {
	err := retryError("payment", 3)
	if err == nil {
		t.Fatal("esperado erro não nulo")
	}
	if !strings.Contains(err.Error(), "payment") || !strings.Contains(err.Error(), "3") {
		t.Errorf("mensagem de erro inesperada: %v", err)
	}
}

// --- InventorySimulator ----------------------------------------------------------

func TestInventorySimulator_DefaultRate(t *testing.T) {
	// taxa 1.0 => sempre reserva; taxa 0.0 => nunca reserva
	always := NewInventorySimulator(1.0)
	reserved, err := always.Reserve(context.Background(), "order-normal")
	if err != nil || !reserved {
		t.Errorf("taxa 1.0 deveria reservar sempre, obtido reserved=%v err=%v", reserved, err)
	}

	never := NewInventorySimulator(0.0)
	reserved, err = never.Reserve(context.Background(), "order-normal-2")
	if err != nil || reserved {
		t.Errorf("taxa 0.0 não deveria reservar, obtido reserved=%v err=%v", reserved, err)
	}
}

func TestInventorySimulator_FailScenario(t *testing.T) {
	s := NewInventorySimulator(1.0)
	reserved, err := s.Reserve(context.Background(), "order-inventory-fail-001")
	if err != nil || reserved {
		t.Errorf("cenário fail deveria retornar false, obtido reserved=%v err=%v", reserved, err)
	}
}

func TestInventorySimulator_RetryOnceScenario(t *testing.T) {
	s := NewInventorySimulator(1.0)
	orderID := "order-inventory-retry-once-001"

	reserved, err := s.Reserve(context.Background(), orderID)
	if err == nil || reserved {
		t.Errorf("1ª tentativa deveria falhar com erro, obtido reserved=%v err=%v", reserved, err)
	}

	reserved, err = s.Reserve(context.Background(), orderID)
	if err != nil || !reserved {
		t.Errorf("2ª tentativa deveria reservar, obtido reserved=%v err=%v", reserved, err)
	}
}

func TestInventorySimulator_RetryScenario(t *testing.T) {
	s := NewInventorySimulator(1.0)
	orderID := "order-inventory-retry-001"

	for i := 1; i <= 3; i++ {
		reserved, err := s.Reserve(context.Background(), orderID)
		if err == nil || reserved {
			t.Errorf("tentativa %d deveria falhar com erro, obtido reserved=%v err=%v", i, reserved, err)
		}
	}
}

// --- NotificationSimulator -------------------------------------------------------

func TestNotificationSimulator_DefaultRate(t *testing.T) {
	always := NewNotificationSimulator(1.0)
	sent, err := always.Notify(context.Background(), "order-normal")
	if err != nil || !sent {
		t.Errorf("taxa 1.0 deveria enviar sempre, obtido sent=%v err=%v", sent, err)
	}

	never := NewNotificationSimulator(0.0)
	sent, err = never.Notify(context.Background(), "order-normal-2")
	if err != nil || sent {
		t.Errorf("taxa 0.0 não deveria enviar, obtido sent=%v err=%v", sent, err)
	}
}

func TestNotificationSimulator_FailScenario(t *testing.T) {
	s := NewNotificationSimulator(1.0)
	sent, err := s.Notify(context.Background(), "order-notification-fail-001")
	if err != nil || sent {
		t.Errorf("cenário fail deveria retornar false, obtido sent=%v err=%v", sent, err)
	}
}

func TestNotificationSimulator_RetryOnceScenario(t *testing.T) {
	s := NewNotificationSimulator(1.0)
	orderID := "order-notification-retry-once-001"

	sent, err := s.Notify(context.Background(), orderID)
	if err == nil || sent {
		t.Errorf("1ª tentativa deveria falhar com erro, obtido sent=%v err=%v", sent, err)
	}

	sent, err = s.Notify(context.Background(), orderID)
	if err != nil || !sent {
		t.Errorf("2ª tentativa deveria enviar, obtido sent=%v err=%v", sent, err)
	}
}

func TestNotificationSimulator_RetryScenario(t *testing.T) {
	s := NewNotificationSimulator(1.0)
	orderID := "order-notification-retry-001"

	for i := 1; i <= 3; i++ {
		sent, err := s.Notify(context.Background(), orderID)
		if err == nil || sent {
			t.Errorf("tentativa %d deveria falhar com erro, obtido sent=%v err=%v", i, sent, err)
		}
	}
}
