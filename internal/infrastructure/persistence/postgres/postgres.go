package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	connectRetries = 10
	connectDelay   = 2 * time.Second
)

// Connect cria um pool de conexões com o banco de escrita e aguarda até que o banco
// responda, retentando enquanto o contexto permitir (útil em debug local quando o
// Postgres ainda está subindo).
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var err error

	for attempt := 1; attempt <= connectRetries; attempt++ {
		pool, err = pgxpool.New(ctx, databaseURL)
		if err == nil {
			var pingErr error
			if pingErr = pool.Ping(ctx); pingErr == nil {
				return pool, nil
			}
			pool.Close()
			err = pingErr
		}

		slog.Warn("retry de conexão com postgres", "component", "postgres", "phase", "connect-retry", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("conexão com postgres interrompida: %w", ctx.Err())
		case <-time.After(connectDelay):
		}
	}

	return nil, fmt.Errorf("não foi possível conectar ao postgres: %w", err)
}
