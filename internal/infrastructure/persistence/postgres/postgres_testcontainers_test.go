//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"workers-kafka/internal/application/orchestrator"
	"workers-kafka/internal/domain"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
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

	pub := &capturingPublisher{}
	orch := orchestrator.New(pub,
		infrapostgres.NewSagaRepository(pool),
		infrapostgres.NewEventLogRepository(pool), 3)

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
