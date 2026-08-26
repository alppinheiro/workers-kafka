package external

import (
	"context"
	"sync"
)

// InventorySimulator simula a chamada a uma API externa de estoque, reservando conforme uma taxa configurável.
type InventorySimulator struct {
	reservationRate float64
	attempts        map[string]int
	// mu protege attempts (goroutines concorrentes com SAGA_WORKERS > 1).
	mu sync.Mutex
}

// NewInventorySimulator cria o simulador com a taxa de sucesso de reserva desejada (entre 0 e 1).
func NewInventorySimulator(reservationRate float64) *InventorySimulator {
	return &InventorySimulator{
		reservationRate: reservationRate,
		attempts:        make(map[string]int),
	}
}

// Reserve simula a reserva de estoque para o pedido informado.
func (i *InventorySimulator) Reserve(_ context.Context, orderID string) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.attempts[orderID]++
	attempt := i.attempts[orderID]

	switch matchScenario(orderID, "inventory") {
	case scenarioFail:
		return false, nil
	case scenarioRetry:
		return false, retryError("inventory", attempt)
	case scenarioRetryOnce:
		if attempt == 1 {
			return false, retryError("inventory", attempt)
		}
		return true, nil
	}

	return randForOrder(orderID).Float64() < i.reservationRate, nil
}
