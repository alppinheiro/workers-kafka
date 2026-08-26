package orchestrator

import (
	"testing"

	"workers-kafka/internal/domain"
)

// TestCanTransitionTo cobre a tabela explícita da máquina de estados (review 3.2):
// transições válidas do fluxo e transições que devem ser rejeitadas (pular etapas,
// regressões e estados terminais).
func TestCanTransitionTo(t *testing.T) {
	valid := [][2]domain.OrderStatus{
		{domain.StatusPending, domain.StatusPaymentPending},
		{domain.StatusPaymentPending, domain.StatusPaymentApproved},
		{domain.StatusPaymentPending, domain.StatusFailed},
		{domain.StatusPaymentApproved, domain.StatusInventoryReserved},
		{domain.StatusPaymentApproved, domain.StatusPaymentRefundPending},
		{domain.StatusPaymentApproved, domain.StatusFailed},
		{domain.StatusPaymentRefundPending, domain.StatusFailed},
		{domain.StatusInventoryReserved, domain.StatusNotified},
		{domain.StatusInventoryReserved, domain.StatusCompleted},
		{domain.StatusInventoryReserved, domain.StatusFailed},
		{domain.StatusNotified, domain.StatusCompleted},
	}
	for _, tr := range valid {
		if !canTransitionTo(tr[0], tr[1]) {
			t.Errorf("esperava transição válida %s → %s", tr[0], tr[1])
		}
	}

	invalid := [][2]domain.OrderStatus{
		{domain.StatusPending, domain.StatusCompleted}, // pular todas as etapas
		{domain.StatusPending, domain.StatusFailed},    // falha só a partir de PAYMENT_PENDING
		{domain.StatusPaymentPending, domain.StatusInventoryReserved},
		{domain.StatusPaymentApproved, domain.StatusNotified},
		{domain.StatusInventoryReserved, domain.StatusPaymentApproved}, // regressão
		{domain.StatusNotified, domain.StatusInventoryReserved},
		{domain.StatusCompleted, domain.StatusFailed}, // terminal é final
	}
	for _, tr := range invalid {
		if canTransitionTo(tr[0], tr[1]) {
			t.Errorf("não esperava transição %s → %s", tr[0], tr[1])
		}
	}
}

func TestAssertTransition(t *testing.T) {
	if err := assertTransition(domain.StatusPaymentPending, domain.StatusPaymentApproved); err != nil {
		t.Fatalf("transição válida deveria passar, got %v", err)
	}
	if err := assertTransition(domain.StatusPending, domain.StatusCompleted); err == nil {
		t.Fatal("transição inválida deveria falhar")
	}
}
