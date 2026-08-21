//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"workers-kafka/internal/application"
	"workers-kafka/internal/application/orchestrator"
	"workers-kafka/internal/domain"
	"workers-kafka/internal/infrastructure/uow"
)

// TestSagaFlowWithPostgresContainer valida o fluxo completo da saga contra um Postgres
// REAL em container (Testcontainers), independente do docker-compose.
func TestSagaFlowWithPostgresContainer(t *testing.T) {
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("saga"),
		postgres.WithUsername("saga"),
		postgres.WithPassword("saga"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("falha ao subir postgres (testcontainers): %v", err)
	}
	defer func() { _ = pg.Terminate(ctx) }()

	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("falha ao obter connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("falha ao conectar no postgres: %v", err)
	}
	defer pool.Close()

	applyMigrations(t, pool)

	orch := orchestrator.New(uow.New(pool), 3)

	orderID := "order-tc-001"
	if err := orch.StartOrder(ctx, flowEvent(orderID, "e-tc-created", domain.EventOrderCreated, domain.StatusPending, "")); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}

	pay := flowEvent(orderID, "e-tc-pay", domain.EventPaymentResult, domain.StatusPaymentApproved, "tx-tc-1")
	if err := orch.HandleResult(ctx, pay); err != nil {
		t.Fatalf("payment falhou: %v", err)
	}
	inv := flowEvent(orderID, "e-tc-inv", domain.EventInventoryResult, domain.StatusInventoryReserved, "")
	if err := orch.HandleResult(ctx, inv); err != nil {
		t.Fatalf("inventory falhou: %v", err)
	}
	notif := flowEvent(orderID, "e-tc-notif", domain.EventNotificationResult, domain.StatusNotified, "")
	if err := orch.HandleResult(ctx, notif); err != nil {
		t.Fatalf("notification falhou: %v", err)
	}

	assertSagaFinal(t, pool, orderID, domain.StatusCompleted)

	// A outbox deve acompanhar as transições da saga (nada de outbox órfã nem ausente).
	var outboxCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE key=$1", orderID).Scan(&outboxCount); err != nil {
		t.Fatalf("falha ao contar outbox do pedido: %v", err)
	}
	if outboxCount == 0 {
		t.Error("esperado ao menos 1 evento na outbox para o pedido concluído")
	}

	got := fetchLogSequence(t, pool, orderID)
	want := []expectedLog{
		{"IN", domain.EventOrderCreated},
		{"OUT", domain.EventPaymentCommand},
		{"IN", domain.EventPaymentResult},
		{"OUT", domain.EventInventoryCommand},
		{"IN", domain.EventInventoryResult},
		{"OUT", domain.EventNotificationCommand},
		{"IN", domain.EventNotificationResult},
		{"OUT", domain.EventOrderCompleted},
	}
	if len(got) != len(want) {
		t.Fatalf("esperados %d registros no journal, obtidos %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("registro %d: esperado %+v, obtido %+v", i, want[i], got[i])
		}
	}
}

// applyMigrations executa os arquivos .up.sql do projeto em ordem.
func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	files := []string{
		"../../../../migrations/000001_create_sagas.up.sql",
		"../../../../migrations/000002_create_saga_events.up.sql",
		"../../../../migrations/000003_create_outbox.up.sql",
		"../../../../migrations/000004_add_outbox_traceparent.up.sql",
		"../../../../migrations/000005_add_outbox_claim.up.sql",
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("falha ao ler migration %s: %v", f, err)
		}
		if _, err := pool.Exec(context.Background(), string(b)); err != nil {
			t.Fatalf("falha ao aplicar migration %s: %v", f, err)
		}
	}
}

// TestUnitOfWorkRollbackWithContainer prova a atomicidade da Etapa 7.4 contra um Postgres
// REAL: as três escritas (estado + journal + outbox) ocorrem na mesma transação e, quando
// o bloco falha, nenhuma delas é persistida (rollback).
func TestUnitOfWorkRollbackWithContainer(t *testing.T) {
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("saga"),
		postgres.WithUsername("saga"),
		postgres.WithPassword("saga"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("falha ao subir postgres (testcontainers): %v", err)
	}
	defer func() { _ = pg.Terminate(ctx) }()

	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("falha ao obter connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("falha ao conectar no postgres: %v", err)
	}
	defer pool.Close()

	applyMigrations(t, pool)

	orderID := "order-tc-rollback"
	saga := domain.Saga{OrderID: orderID, Current: domain.StatusPaymentPending}
	entry := application.EventLogEntry{
		OrderID: orderID, SagaID: orderID, EventID: "e-tc-rollback",
		EventType: domain.EventPaymentCommand, Component: "orchestrator",
		Direction: application.DirectionOut, StatusAtual: domain.StatusPaymentPending,
	}
	command := domain.Event{
		EventID:       "e-tc-rollback",
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   domain.StatusPaymentPending,
		EventType:     domain.EventPaymentCommand,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}

	err = uow.New(pool).WithTx(ctx, func(tx application.SagaTx) error {
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

	for _, table := range []struct{ tbl, col, val string }{
		{"sagas", "order_id", orderID},
		{"saga_events", "order_id", orderID},
		{"outbox", "event_id", "e-tc-rollback"},
	} {
		var got int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table.tbl+" WHERE "+table.col+" = $1", table.val).Scan(&got); err != nil {
			t.Fatalf("falha ao contar %s: %v", table.tbl, err)
		}
		if got != 0 {
			t.Errorf("%s: %s=%s: rollback não desfez a escrita (count=%d)", table.tbl, table.col, table.val, got)
		}
	}
}
