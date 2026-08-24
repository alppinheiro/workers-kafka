package external

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// PaymentSimulator simula a chamada a uma API externa de pagamento, aprovando conforme uma taxa configurável.
type PaymentSimulator struct {
	approvalRate   float64
	attempts       map[string]int
	refundAttempts map[string]int
	rng            *rand.Rand
	// mu protege attempts/refundAttempts/rng: os workers processam eventos em
	// goroutines concorrentes (SAGA_WORKERS > 1) e map/rand não são thread-safe.
	mu sync.Mutex
}

// NewPaymentSimulator cria o simulador com a taxa de aprovação desejada (entre 0 e 1).
func NewPaymentSimulator(approvalRate float64) *PaymentSimulator {
	return &PaymentSimulator{
		approvalRate:   approvalRate,
		attempts:       make(map[string]int),
		refundAttempts: make(map[string]int),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Process simula a validação do pagamento do pedido informado.
func (p *PaymentSimulator) Process(_ context.Context, orderID string) (bool, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.attempts[orderID]++
	attempt := p.attempts[orderID]

	switch matchScenario(orderID, "payment") {
	case scenarioFail:
		return false, "", nil
	case scenarioRetry:
		return false, "", retryError("payment", attempt)
	case scenarioRetryOnce:
		if attempt == 1 {
			return false, "", retryError("payment", attempt)
		}
		tx := fmt.Sprintf("tx-%s-%d", orderID, time.Now().UnixNano())
		return true, tx, nil
	}

	if p.rng.Float64() < p.approvalRate {
		tx := fmt.Sprintf("tx-%s-%d", orderID, time.Now().UnixNano())
		return true, tx, nil
	}
	return false, "", nil
}

// Refund simula o estorno do pagamento identificado pelo transactionID.
func (p *PaymentSimulator) Refund(_ context.Context, orderID string, transactionID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.refundAttempts[orderID]++
	attempt := p.refundAttempts[orderID]

	// Reuse matchScenario on payment stage to allow deterministic refund scenarios when desired.
	switch matchScenario(orderID, "payment") {
	case scenarioFail:
		return false, nil
	case scenarioRetry:
		return false, retryError("refund", attempt)
	case scenarioRetryOnce:
		if attempt == 1 {
			return false, retryError("refund", attempt)
		}
		return true, nil
	}

	// Default: refund succeeds according to approvalRate (higher chance to succeed)
	if p.rng.Float64() < p.approvalRate {
		return true, nil
	}
	return false, nil
}
