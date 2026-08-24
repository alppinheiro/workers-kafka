package external

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// NotificationSimulator simula a chamada a uma API externa de notificação, enviando conforme uma taxa configurável.
type NotificationSimulator struct {
	deliveryRate float64
	attempts     map[string]int
	rng          *rand.Rand
	// mu protege attempts/rng (goroutines concorrentes com SAGA_WORKERS > 1).
	mu sync.Mutex
}

// NewNotificationSimulator cria o simulador com a taxa de sucesso de envio desejada (entre 0 e 1).
func NewNotificationSimulator(deliveryRate float64) *NotificationSimulator {
	return &NotificationSimulator{
		deliveryRate: deliveryRate,
		attempts:     make(map[string]int),
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Notify simula o envio da notificação de conclusão do pedido informado.
func (n *NotificationSimulator) Notify(_ context.Context, orderID string) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.attempts[orderID]++
	attempt := n.attempts[orderID]

	switch matchScenario(orderID, "notification") {
	case scenarioFail:
		return false, nil
	case scenarioRetry:
		return false, retryError("notification", attempt)
	case scenarioRetryOnce:
		if attempt == 1 {
			return false, retryError("notification", attempt)
		}
		return true, nil
	}

	return n.rng.Float64() < n.deliveryRate, nil
}
