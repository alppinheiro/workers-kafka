package external

import (
	"context"
	"math/rand"
	"time"
)

// PaymentSimulator simula a chamada a uma API externa de pagamento, aprovando conforme uma taxa configurável.
type PaymentSimulator struct {
	approvalRate float64
	attempts     map[string]int
	rng          *rand.Rand
}

// NewPaymentSimulator cria o simulador com a taxa de aprovação desejada (entre 0 e 1).
func NewPaymentSimulator(approvalRate float64) *PaymentSimulator {
	return &PaymentSimulator{
		approvalRate: approvalRate,
		attempts:     make(map[string]int),
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Process simula a validação do pagamento do pedido informado.
func (p *PaymentSimulator) Process(_ context.Context, orderID string) (bool, error) {
	p.attempts[orderID]++
	attempt := p.attempts[orderID]

	switch matchScenario(orderID, "payment") {
	case scenarioFail:
		return false, nil
	case scenarioRetry:
		return false, retryError("payment", attempt)
	case scenarioRetryOnce:
		if attempt == 1 {
			return false, retryError("payment", attempt)
		}
		return true, nil
	}

	return p.rng.Float64() < p.approvalRate, nil
}
