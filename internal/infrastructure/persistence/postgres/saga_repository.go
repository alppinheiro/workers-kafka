package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// SagaRepository persiste o estado corrente da saga no banco de escrita.
type SagaRepository struct {
	pool *pgxpool.Pool
}

// NewSagaRepository cria o repositório de sagas sobre o pool informado.
func NewSagaRepository(pool *pgxpool.Pool) *SagaRepository {
	return &SagaRepository{pool: pool}
}

// Save insere ou atualiza (upsert) o estado da saga identificada por OrderID.
func (r *SagaRepository) Save(ctx context.Context, saga domain.Saga) error {
	const query = `
INSERT INTO sagas (order_id, saga_id, current_status, previous_status, retry_count, transaction_id, updated_at)
VALUES ($1, $1, $2, $3, $4, $5, now())
ON CONFLICT (order_id) DO UPDATE SET
	current_status  = EXCLUDED.current_status,
	previous_status = EXCLUDED.previous_status,
	retry_count     = EXCLUDED.retry_count,
	transaction_id  = EXCLUDED.transaction_id,
	updated_at      = now()`

	_, err := r.pool.Exec(ctx, query,
		saga.OrderID,
		string(saga.Current),
		string(saga.Previous),
		saga.RetryCount,
		saga.TransactionID,
	)
	if err != nil {
		return fmt.Errorf("erro ao salvar saga %s: %w", saga.OrderID, err)
	}
	return nil
}

// Load recupera o estado corrente da saga; retorna application.ErrSagaNotFound se não existir.
func (r *SagaRepository) Load(ctx context.Context, orderID string) (domain.Saga, error) {
	const query = `
SELECT order_id, previous_status, current_status, retry_count, COALESCE(transaction_id, '')
FROM sagas
WHERE order_id = $1`

	var saga domain.Saga
	err := r.pool.QueryRow(ctx, query, orderID).Scan(
		&saga.OrderID,
		&saga.Previous,
		&saga.Current,
		&saga.RetryCount,
		&saga.TransactionID,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.Saga{}, application.ErrSagaNotFound
		}
		return domain.Saga{}, fmt.Errorf("erro ao carregar saga %s: %w", orderID, err)
	}
	return saga, nil
}
