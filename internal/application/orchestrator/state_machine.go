package orchestrator

import (
	"fmt"

	"workers-kafka/internal/domain"
)

// validTransitions é a tabela explícita da máquina de estados da saga (review 3.2):
// de qual status a saga pode avançar para qual. Tornar as regras visíveis como dado
// permite auditar e testar isoladamente — um novo estado/transição aparece
// obrigatoriamente aqui (as validações assertTransition falham se faltar).
var validTransitions = map[domain.OrderStatus][]domain.OrderStatus{
	domain.StatusPending: {
		domain.StatusPaymentPending,
	},
	domain.StatusPaymentPending: {
		domain.StatusPaymentApproved,
		domain.StatusFailed, // falha definitiva de pagamento (sem compensação)
	},
	domain.StatusPaymentApproved: {
		domain.StatusInventoryReserved,
		domain.StatusPaymentRefundPending, // compensação (falha de estoque com tx)
		domain.StatusFailed,
	},
	domain.StatusPaymentRefundPending: {
		domain.StatusFailed, // estorno concluído (PAYMENT_REFUNDED) encerra a saga
	},
	domain.StatusInventoryReserved: {
		domain.StatusNotified,  // NOTIFICATION_RESULT aprovado
		domain.StatusCompleted, // falha de notificação ignorada (special-case)
		domain.StatusFailed,    // retry excedido
	},
	domain.StatusNotified: {
		domain.StatusCompleted,
	},
}

// canTransitionTo informa se a saga pode avançar de from para to segundo a tabela.
func canTransitionTo(from, to domain.OrderStatus) bool {
	for _, next := range validTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// assertTransition valida uma transição contra a tabela; retorna erro definitivo quando
// a transição não é permitida (protege contra novos estados não registrados).
func assertTransition(from, to domain.OrderStatus) error {
	if !canTransitionTo(from, to) {
		return fmt.Errorf("%w: transição inválida da saga %s → %s (atualize validTransitions)", domain.ErrInvalidTransition, from, to)
	}
	return nil
}
