package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// DBTX é o contrato mínimo de execução SQL compartilhado por *pgxpool.Pool (execução
// avulsa) e pgx.Tx (transação). Com ele os repositórios operam tanto fora quanto dentro
// de uma transação, habilitando a atomicidade de escrita (estado + journal + outbox).
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// SagaRepository persiste o estado corrente da saga no banco de escrita.
type SagaRepository struct {
	db DBTX
}

// NewSagaRepository cria o repositório de sagas sobre o pool informado.
func NewSagaRepository(pool *pgxpool.Pool) *SagaRepository {
	return &SagaRepository{db: pool}
}

// NewSagaRepositoryTx cria o repositório de sagas vinculado a uma transação: todas as
// operações executam dentro do pgx.Tx, junto com journal e outbox (atomicidade).
func NewSagaRepositoryTx(tx pgx.Tx) *SagaRepository {
	return &SagaRepository{db: tx}
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

	_, err := r.db.Exec(ctx, query,
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
	err := r.db.QueryRow(ctx, query, orderID).Scan(
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
