package external

import (
	"context"
	"strings"
	"testing"
)

// --- PaymentSimulator: Process ----------------------------------------------------

func TestPaymentSimulator_Process_DefaultRate(t *testing.T) {
	always := NewPaymentSimulator(1.0)
	approved, tx, err := always.Process(context.Background(), "order-normal")
	if err != nil || !approved {
		t.Fatalf("taxa 1.0 deveria aprovar sempre, obtido approved=%v err=%v", approved, err)
	}
	if !strings.HasPrefix(tx, "tx-") {
		t.Errorf("transactionID com prefixo inesperado: %q", tx)
	}

	never := NewPaymentSimulator(0.0)
	approved, _, err = never.Process(context.Background(), "order-normal-2")
	if err != nil || approved {
		t.Errorf("taxa 0.0 não deveria aprovar, obtido approved=%v err=%v", approved, err)
	}
}

func TestPaymentSimulator_Process_FailScenario(t *testing.T) {
	s := NewPaymentSimulator(1.0)
	approved, tx, err := s.Process(context.Background(), "order-payment-fail-001")
	if err != nil || approved {
		t.Errorf("cenário fail deveria retornar false, obtido approved=%v err=%v", approved, err)
	}
	if tx != "" {
		t.Errorf("cenário fail não deveria gerar transactionID, obtido %q", tx)
	}
}

func TestPaymentSimulator_Process_RetryOnceScenario(t *testing.T) {
	s := NewPaymentSimulator(1.0)
	orderID := "order-payment-retry-once-001"

	approved, _, err := s.Process(context.Background(), orderID)
	if err == nil || approved {
		t.Errorf("1ª tentativa deveria falhar com erro, obtido approved=%v err=%v", approved, err)
	}

	approved, tx, err := s.Process(context.Background(), orderID)
	if err != nil || !approved {
		t.Errorf("2ª tentativa deveria aprovar, obtido approved=%v err=%v", approved, err)
	}
	if !strings.HasPrefix(tx, "tx-") {
		t.Errorf("transactionID com prefixo inesperado: %q", tx)
	}
}

func TestPaymentSimulator_Process_RetryScenario(t *testing.T) {
	s := NewPaymentSimulator(1.0)
	orderID := "order-payment-retry-001"

	for i := 1; i <= 3; i++ {
		approved, _, err := s.Process(context.Background(), orderID)
		if err == nil || approved {
			t.Errorf("tentativa %d deveria falhar com erro, obtido approved=%v err=%v", i, approved, err)
		}
	}
}

// --- PaymentSimulator: Refund -------------------------------------------------------

func TestPaymentSimulator_Refund_DefaultRate(t *testing.T) {
	always := NewPaymentSimulator(1.0)
	refunded, err := always.Refund(context.Background(), "order-normal", "tx-1")
	if err != nil || !refunded {
		t.Errorf("taxa 1.0 deveria estornar sempre, obtido refunded=%v err=%v", refunded, err)
	}

	never := NewPaymentSimulator(0.0)
	refunded, err = never.Refund(context.Background(), "order-normal-2", "tx-2")
	if err != nil || refunded {
		t.Errorf("taxa 0.0 não deveria estornar, obtido refunded=%v err=%v", refunded, err)
	}
}

func TestPaymentSimulator_Refund_FailScenario(t *testing.T) {
	s := NewPaymentSimulator(1.0)
	refunded, err := s.Refund(context.Background(), "order-payment-fail-001", "tx-1")
	if err != nil || refunded {
		t.Errorf("cenário fail deveria retornar false, obtido refunded=%v err=%v", refunded, err)
	}
}

func TestPaymentSimulator_Refund_RetryOnceScenario(t *testing.T) {
	s := NewPaymentSimulator(1.0)
	orderID := "order-payment-retry-once-001"

	refunded, err := s.Refund(context.Background(), orderID, "tx-1")
	if err == nil || refunded {
		t.Errorf("1ª tentativa deveria falhar com erro, obtido refunded=%v err=%v", refunded, err)
	}

	refunded, err = s.Refund(context.Background(), orderID, "tx-1")
	if err != nil || !refunded {
		t.Errorf("2ª tentativa deveria estornar, obtido refunded=%v err=%v", refunded, err)
	}
}

func TestPaymentSimulator_Refund_RetryScenario(t *testing.T) {
	s := NewPaymentSimulator(1.0)
	orderID := "order-payment-retry-001"

	for i := 1; i <= 3; i++ {
		refunded, err := s.Refund(context.Background(), orderID, "tx-1")
		if err == nil || refunded {
			t.Errorf("tentativa %d deveria falhar com erro, obtido refunded=%v err=%v", i, refunded, err)
		}
	}
}
