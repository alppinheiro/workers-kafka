package postgres

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// newTestPool conecta no Postgres apontado por DATABASE_URL ou pula o teste de
// integração quando a variável não está definida (ex.: CI sem banco).
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL não definida; pulando teste de integração")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("falha ao conectar no postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// cleanSagas remove as linhas das tabelas de escrita para isolamento dos testes.
func cleanSagas(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"saga_events", "sagas"} {
		if _, err := pool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("falha ao limpar %s: %v", table, err)
		}
	}
}

func TestSagaRepository_SaveAndLoad(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewSagaRepository(pool)

	want := domain.Saga{
		OrderID:       "order-it-001",
		Previous:      domain.StatusPending,
		Current:       domain.StatusPaymentPending,
		RetryCount:    2,
		TransactionID: "tx-it-1",
	}

	if err := repo.Save(ctx, want); err != nil {
		t.Fatalf("Save falhou: %v", err)
	}

	got, err := repo.Load(ctx, "order-it-001")
	if err != nil {
		t.Fatalf("Load falhou: %v", err)
	}
	if got != want {
		t.Errorf("saga persistida incorreta: got=%+v want=%+v", got, want)
	}
}

func TestSagaRepository_SaveIsUpsert(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewSagaRepository(pool)

	first := domain.Saga{OrderID: "order-it-002", Current: domain.StatusPaymentPending}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("primeiro Save falhou: %v", err)
	}

	updated := domain.Saga{OrderID: "order-it-002", Previous: domain.StatusPaymentPending, Current: domain.StatusPaymentApproved, TransactionID: "tx-it-2"}
	if err := repo.Save(ctx, updated); err != nil {
		t.Fatalf("segundo Save falhou: %v", err)
	}

	got, err := repo.Load(ctx, "order-it-002")
	if err != nil {
		t.Fatalf("Load falhou: %v", err)
	}
	if got.Current != domain.StatusPaymentApproved || got.Previous != domain.StatusPaymentPending {
		t.Errorf("upsert não refletiu a atualização: %+v", got)
	}
}

func TestSagaRepository_LoadNotFound(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewSagaRepository(pool)

	_, err := repo.Load(ctx, "order-inexistente")
	if !errors.Is(err, application.ErrSagaNotFound) {
		t.Fatalf("esperado ErrSagaNotFound, obtido %v", err)
	}
}
