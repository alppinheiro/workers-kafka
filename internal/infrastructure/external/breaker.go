package external

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/sony/gobreaker"

	"workers-kafka/internal/application"
)

// breakerConfig define o comportamento do circuit breaker aplicado aos gateways
// externos. Configurável via ambiente (defaults pensados para não interferir nos
// cenários de estudo — o orquestrador faz no máximo 3 retries por saga, abaixo do
// threshold de 5 falhas consecutivas):
//
//	GATEWAY_CB_ENABLED      (default true)
//	GATEWAY_CB_MAX_FAILURES (default 5)
//	GATEWAY_CB_TIMEOUT      (default 10s — tempo em "open" antes de half-open)
type breakerConfig struct {
	enabled     bool
	maxFailures int
	timeout     time.Duration
}

func breakerConfigFromEnv() breakerConfig {
	cfg := breakerConfig{enabled: true, maxFailures: 5, timeout: 10 * time.Second}

	if v := os.Getenv("GATEWAY_CB_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.enabled = b
		}
	}
	if v := os.Getenv("GATEWAY_CB_MAX_FAILURES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			cfg.maxFailures = n
		}
	}
	if v := os.Getenv("GATEWAY_CB_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.timeout = d
		}
	}
	return cfg
}

func (c breakerConfig) settings(name string) gobreaker.Settings {
	return gobreaker.Settings{
		Name:        name,
		MaxRequests: 1, // half-open: apenas 1 request de teste
		Timeout:     c.timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return int(counts.Requests) >= c.maxFailures && int(counts.ConsecutiveFailures) >= c.maxFailures
		},
	}
}

// --- Payment -----------------------------------------------------------------

type paymentResult struct {
	approved bool
	tx       string
}

type paymentGatewayBreaker struct {
	inner application.PaymentGateway
	cb    *gobreaker.CircuitBreaker
}

// PaymentGatewayFromEnv aplica um circuit breaker ao gateway de pagamento.
// Retorna o gateway original quando GATEWAY_CB_ENABLED=false (debug/estudo).
func PaymentGatewayFromEnv(inner application.PaymentGateway) application.PaymentGateway {
	return withPaymentBreaker(inner, breakerConfigFromEnv())
}

func withPaymentBreaker(inner application.PaymentGateway, cfg breakerConfig) application.PaymentGateway {
	if !cfg.enabled {
		return inner
	}
	return &paymentGatewayBreaker{
		inner: inner,
		cb:    gobreaker.NewCircuitBreaker(cfg.settings("payment")),
	}
}

func (b *paymentGatewayBreaker) Process(ctx context.Context, orderID string) (bool, string, error) {
	v, err := b.cb.Execute(func() (any, error) {
		approved, tx, err := b.inner.Process(ctx, orderID)
		if err != nil {
			return nil, err
		}
		return paymentResult{approved: approved, tx: tx}, nil
	})
	if err != nil {
		return false, "", err
	}
	r := v.(paymentResult)
	return r.approved, r.tx, nil
}

func (b *paymentGatewayBreaker) Refund(ctx context.Context, orderID string, transactionID string) (bool, error) {
	v, err := b.cb.Execute(func() (any, error) {
		refunded, err := b.inner.Refund(ctx, orderID, transactionID)
		if err != nil {
			return nil, err
		}
		return refunded, nil
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

// --- Inventory ---------------------------------------------------------------

type inventoryGatewayBreaker struct {
	inner application.InventoryGateway
	cb    *gobreaker.CircuitBreaker
}

// InventoryGatewayFromEnv aplica um circuit breaker ao gateway de estoque.
func InventoryGatewayFromEnv(inner application.InventoryGateway) application.InventoryGateway {
	return withInventoryBreaker(inner, breakerConfigFromEnv())
}

func withInventoryBreaker(inner application.InventoryGateway, cfg breakerConfig) application.InventoryGateway {
	if !cfg.enabled {
		return inner
	}
	return &inventoryGatewayBreaker{
		inner: inner,
		cb:    gobreaker.NewCircuitBreaker(cfg.settings("inventory")),
	}
}

func (b *inventoryGatewayBreaker) Reserve(ctx context.Context, orderID string) (bool, error) {
	v, err := b.cb.Execute(func() (any, error) {
		reserved, err := b.inner.Reserve(ctx, orderID)
		if err != nil {
			return nil, err
		}
		return reserved, nil
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

// --- Notification ------------------------------------------------------------

type notificationGatewayBreaker struct {
	inner application.NotificationGateway
	cb    *gobreaker.CircuitBreaker
}

// NotificationGatewayFromEnv aplica um circuit breaker ao gateway de notificação.
func NotificationGatewayFromEnv(inner application.NotificationGateway) application.NotificationGateway {
	return withNotificationBreaker(inner, breakerConfigFromEnv())
}

func withNotificationBreaker(inner application.NotificationGateway, cfg breakerConfig) application.NotificationGateway {
	if !cfg.enabled {
		return inner
	}
	return &notificationGatewayBreaker{
		inner: inner,
		cb:    gobreaker.NewCircuitBreaker(cfg.settings("notification")),
	}
}

func (b *notificationGatewayBreaker) Notify(ctx context.Context, orderID string) (bool, error) {
	v, err := b.cb.Execute(func() (any, error) {
		sent, err := b.inner.Notify(ctx, orderID)
		if err != nil {
			return nil, err
		}
		return sent, nil
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}
