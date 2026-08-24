package external

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestSimulatorsConcurrentSafety garante que os simuladores são seguros para uso
// concorrente (SAGA_WORKERS > 1). Rodado com -race, qualquer acesso não protegido
// a attempts/rng (ex.: concurrent map write) dispara o detector — regressão do
// ponto 2.2 do TECHNICAL_REVIEW.
func TestSimulatorsConcurrentSafety(t *testing.T) {
	payment := NewPaymentSimulator(1.0)
	inventory := NewInventorySimulator(1.0)
	notification := NewNotificationSimulator(1.0)

	const goroutines = 16
	const ordersPerGoroutine = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx := context.Background()
			for o := 0; o < ordersPerGoroutine; o++ {
				orderID := fmt.Sprintf("order-%d-%d", g, o)

				if _, _, err := payment.Process(ctx, orderID); err != nil {
					t.Errorf("payment.Process(%s): %v", orderID, err)
				}
				if _, err := payment.Refund(ctx, orderID, "tx"); err != nil {
					t.Errorf("payment.Refund(%s): %v", orderID, err)
				}
				if _, err := inventory.Reserve(ctx, orderID); err != nil {
					t.Errorf("inventory.Reserve(%s): %v", orderID, err)
				}
				if _, err := notification.Notify(ctx, orderID); err != nil {
					t.Errorf("notification.Notify(%s): %v", orderID, err)
				}
			}
		}(g)
	}
	wg.Wait()
}
