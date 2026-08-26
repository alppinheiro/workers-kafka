package external

import (
	"context"
	"testing"
)

// TestRandForOrder_Deterministic garante que o mesmo orderID produz o MESMO resultado
// (aprovado/recusado) em qualquer instância do simulador — consistente em scale
// horizontal (review 2.3).
func TestRandForOrder_Deterministic(t *testing.T) {
	payment := NewPaymentSimulator(0.85)

	// Duas instâncias "diferentes" (como dois pods do worker) decidem igual para o
	// mesmo orderID.
	first, _, err := payment.Process(context.Background(), "order-det-001")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	secondSim := NewPaymentSimulator(0.85)
	second, _, err := secondSim.Process(context.Background(), "order-det-001")
	if err != nil {
		t.Fatalf("Process (2ª instância): %v", err)
	}

	if first != second {
		t.Fatalf("mesmo orderID deveria ter o mesmo resultado entre instâncias: %v vs %v", first, second)
	}
}
