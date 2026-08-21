// Package uow (Unit of Work) processa cada evento com estado + journal + outbox em UMA
// transação do banco de escrita: ou todas as escritas são commitadas juntas, ou nenhuma
// (rollback). Elimina as janelas residuais que antes eram cobertas apenas por
// idempotência (event_id).
package uow

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application"
	"workers-kafka/internal/infrastructure/outbox"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
)

// PostgresUnitOfWork é a implementação transacional de application.SagaUnitOfWork sobre
// o Postgres de escrita.
type PostgresUnitOfWork struct {
	pool *pgxpool.Pool
}

// New cria um PostgresUnitOfWork sobre o pool do banco de escrita.
func New(pool *pgxpool.Pool) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{pool: pool}
}

// WithTx inicia uma transação e executa fn com repositórios transacionais (sagas,
// journal e outbox compartilham o mesmo pgx.Tx). Commit se fn retornar nil; qualquer
// erro desfaz todas as escritas (rollback).
func (u *PostgresUnitOfWork) WithTx(ctx context.Context, fn func(tx application.SagaTx) error) error {
	tx, err := u.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("erro ao iniciar transação: %w", err)
	}
	// Rollback seguro: no-op após o Commit (a transação já foi encerrada).
	defer func() { _ = tx.Rollback(ctx) }()

	sagaTx := application.SagaTx{
		Sagas:     infrapostgres.NewSagaRepositoryTx(tx),
		EventLog:  infrapostgres.NewEventLogRepositoryTx(tx),
		Publisher: outbox.NewPublisher(infrapostgres.NewOutboxRepositoryTx(tx)),
	}
	if err := fn(sagaTx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("erro ao commitar transação: %w", err)
	}
	return nil
}
