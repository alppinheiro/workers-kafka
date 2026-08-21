package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
	"workers-kafka/internal/infrastructure/uow"
)

// Estes testes provam a atomicidade da Etapa 7.4: estado (sagas), journal (saga_events)
// e outbox são gravados em UMA transação. Commit persiste as três juntas; qualquer erro
// no bloco desfaz tudo (rollback) — não existe "evento sem estado" nem "outbox órfã".
// Requer DATABASE_URL com as migrations aplicadas; sem a variável, os testes são pulados.

func TestUnitOfWork_AtomicCommit(t *testing.T) {
	pool := newFlowPool(t)
	cleanFlowTables(t, pool)
	ctx := context.Background()

	orderID := "uow-commit"
	saga := domain.Saga{OrderID: orderID, Current: domain.StatusPaymentPending}
	entry := application.EventLogEntry{
		OrderID: orderID, SagaID: orderID, EventID: "uow-commit-evt",
		EventType: domain.EventPaymentCommand, Component: "orchestrator",
		Direction: application.DirectionOut, StatusAtual: domain.StatusPaymentPending,
	}
	command := domain.Event{
		EventID:       "uow-commit-evt",
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   domain.StatusPaymentPending,
		EventType:     domain.EventPaymentCommand,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}

	if err := uow.New(pool).WithTx(ctx, func(tx application.SagaTx) error {
		if err := tx.Sagas.Save(ctx, saga); err != nil {
			return err
		}
		if err := tx.EventLog.Append(ctx, entry); err != nil {
			return err
		}
		return tx.Publisher.Publish(ctx, command)
	}); err != nil {
		t.Fatalf("WithTx (commit) falhou: %v", err)
	}

	assertTableCount(t, pool, "sagas", "order_id", orderID, 1)
	assertTableCount(t, pool, "saga_events", "order_id", orderID, 1)
	assertTableCount(t, pool, "outbox", "event_id", "uow-commit-evt", 1)
}

func TestUnitOfWork_AtomicRollback(t *testing.T) {
	pool := newFlowPool(t)
	cleanFlowTables(t, pool)
	ctx := context.Background()

	orderID := "uow-rollback"
	saga := domain.Saga{OrderID: orderID, Current: domain.StatusPaymentPending}
	entry := application.EventLogEntry{
		OrderID: orderID, SagaID: orderID, EventID: "uow-rollback-evt",
		EventType: domain.EventPaymentCommand, Component: "orchestrator",
		Direction: application.DirectionOut, StatusAtual: domain.StatusPaymentPending,
	}
	command := domain.Event{
		EventID:       "uow-rollback-evt",
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   domain.StatusPaymentPending,
		EventType:     domain.EventPaymentCommand,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}

	// As três escritas acontecem dentro da transação, mas o bloco retorna erro →
	// tudo deve ser desfeito (rollback). É o comportamento que protege contra
	// "saga salva sem comando na outbox" e "outbox sem registro no journal".
	err := uow.New(pool).WithTx(ctx, func(tx application.SagaTx) error {
		if err := tx.Sagas.Save(ctx, saga); err != nil {
			return err
		}
		if err := tx.EventLog.Append(ctx, entry); err != nil {
			return err
		}
		if err := tx.Publisher.Publish(ctx, command); err != nil {
			return err
		}
		return errors.New("erro simulado: aborta a transação")
	})
	if err == nil {
		t.Fatal("esperado erro do bloco transacional")
	}

	assertTableCount(t, pool, "sagas", "order_id", orderID, 0)
	assertTableCount(t, pool, "saga_events", "order_id", orderID, 0)
	assertTableCount(t, pool, "outbox", "event_id", "uow-rollback-evt", 0)
}

// assertTableCount verifica quantas linhas existem na tabela com o filtro fornecido.
func assertTableCount(t *testing.T, pool *pgxpool.Pool, table, column, value string, want int) {
	t.Helper()
	ctx := context.Background()
	query := "SELECT count(*) FROM " + table + " WHERE " + column + " = $1"
	var got int
	if err := pool.QueryRow(ctx, query, value).Scan(&got); err != nil {
		t.Fatalf("falha ao contar %s: %v", table, err)
	}
	if got != want {
		t.Errorf("%s: %s=%s: esperado %d linha(s), obtido %d", table, column, value, want, got)
	}
}
