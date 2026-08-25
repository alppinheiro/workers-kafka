package external

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

// fakePaymentGateway é um gateway de teste com falhas controláveis.
type fakePaymentGateway struct {
	mu        sync.Mutex
	failAll   bool
	failFirst int32
	calls     int32
}

func (f *fakePaymentGateway) Process(_ context.Context, _ string) (bool, string, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAll {
		return false, "", errors.New("gateway indisponível")
	}
	if f.failFirst > 0 {
		f.failFirst--
		return false, "", errors.New("falha temporária")
	}
	return true, "tx-ok", nil
}

func (f *fakePaymentGateway) Refund(_ context.Context, _ string, _ string) (bool, error) {
	return true, nil
}

// waitUntil aguarda cond até timeout (polling curto), falhando o teste caso contrário.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condição não satisfeita a tempo")
}

func TestPaymentBreaker_OpensAfterConsecutiveFailures(t *testing.T) {
	inner := &fakePaymentGateway{failAll: true}
	cfg := breakerConfig{enabled: true, maxFailures: 3, timeout: 10 * time.Second}
	gateway := withPaymentBreaker(inner, cfg)
	b := gateway.(*paymentGatewayBreaker)

	for i := 0; i < 3; i++ {
		if _, _, err := b.Process(context.Background(), "order-1"); err == nil {
			t.Fatalf("chamada %d deveria falhar", i+1)
		}
	}
	if b.cb.State() != gobreaker.StateOpen {
		t.Fatalf("circuito deveria estar OPEN após 3 falhas, state=%v", b.cb.State())
	}

	// Circuito aberto: fail-fast sem tocar o inner.
	_, _, err := b.Process(context.Background(), "order-1")
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("esperava ErrOpenState, got %v", err)
	}
	if got := atomic.LoadInt32(&inner.calls); got != 3 {
		t.Fatalf("inner deveria ter sido chamado 3x, got %d", got)
	}
}

func TestPaymentBreaker_RecoversAfterTimeout(t *testing.T) {
	inner := &fakePaymentGateway{failFirst: 3}
	cfg := breakerConfig{enabled: true, maxFailures: 3, timeout: 50 * time.Millisecond}
	gateway := withPaymentBreaker(inner, cfg)
	b := gateway.(*paymentGatewayBreaker)

	for i := 0; i < 3; i++ {
		if _, _, err := b.Process(context.Background(), "order-1"); err == nil {
			t.Fatalf("chamada %d deveria falhar", i+1)
		}
	}
	if b.cb.State() != gobreaker.StateOpen {
		t.Fatalf("circuito deveria estar OPEN, state=%v", b.cb.State())
	}

	// Aguarda o timeout → half-open (1 request de teste: sucesso fecha o circuito).
	waitUntil(t, 2*time.Second, func() bool { return b.cb.State() == gobreaker.StateHalfOpen })

	approved, _, err := b.Process(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("chamada de teste em half-open deveria passar, got %v", err)
	}
	if !approved {
		t.Fatal("esperava approved=true")
	}
	if b.cb.State() != gobreaker.StateClosed {
		t.Fatalf("sucesso em half-open deveria fechar o circuito, state=%v", b.cb.State())
	}

	// Circuito fechado: novas chamadas fluem normalmente.
	if _, _, err := b.Process(context.Background(), "order-1"); err != nil {
		t.Fatalf("circuito deveria estar saudável, got %v", err)
	}
}

func TestBreakerDisabledReturnsInner(t *testing.T) {
	inner := &fakePaymentGateway{}
	cfg := breakerConfig{enabled: false}
	gateway := withPaymentBreaker(inner, cfg)
	if gateway != inner {
		t.Fatal("GATEWAY_CB_ENABLED=false deveria retornar o gateway original sem wrapper")
	}
}
