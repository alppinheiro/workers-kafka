package health

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	infrakafka "workers-kafka/internal/infrastructure/kafka"
)

// HealthCheck verifica a saúde de uma dependência; retorna erro quando indisponível.
type HealthCheck func(ctx context.Context) error

// checkTimeout é o tempo máximo que cada check individual pode levar.
const checkTimeout = 2 * time.Second

// Pinger abstrai o Ping do pool de conexões Postgres (pgxpool.Pool) sem acoplar o
// pacote health ao driver.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Postgres retorna uma check que valida a conectividade com o Postgres via Ping do
// pool (equivalente a um SELECT 1 no banco de escrita ou de leitura).
func Postgres(pool Pinger) HealthCheck {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		return nil
	}
}

// Kafka retorna uma check que valida a conectividade com o cluster Kafka
// (dial com handshake em qualquer broker).
func Kafka(brokers []string) HealthCheck {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		if err := infrakafka.PingBrokers(ctx, brokers); err != nil {
			return err
		}
		return nil
	}
}

// LastActivity retorna uma check que falha quando nenhum ciclo de trabalho concluiu
// há mais de staleAfter (detecta stall do loop principal, ex.: outbox-relay).
func LastActivity(last *atomic.Int64, staleAfter time.Duration) HealthCheck {
	return func(ctx context.Context) error {
		lastNano := last.Load()
		if lastNano == 0 {
			return errors.New("nenhum ciclo de trabalho concluído")
		}
		age := time.Since(time.Unix(0, lastNano))
		if age > staleAfter {
			return fmt.Errorf("sem atividade há %s (limite %s)", age.Round(time.Second), staleAfter)
		}
		return nil
	}
}
