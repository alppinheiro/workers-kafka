package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application/orchestrator"
	"workers-kafka/internal/domain"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/uow"
)

// Este arquivo prova a persistência real do fluxo da saga: orquestrador com os
// repositórios PostgreSQL reais, verificando que TODOS os eventos de cada etapa são
// gravados em saga_events e o estado final em sagas. Requer DATABASE_URL (migrations
// aplicadas); sem a variável, os testes são pulados.

func newFlowPool(t *testing.T) *pgxpool.Pool {
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

func cleanFlowTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"saga_events", "sagas", "outbox"} {
		if _, err := pool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("falha ao limpar %s: %v", table, err)
		}
	}
}

func flowEvent(orderID, eventID string, eventType domain.EventType, status domain.OrderStatus, txID string) domain.Event {
	return domain.Event{
		EventID:       eventID,
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   status,
		EventType:     eventType,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
		TransactionID: txID,
	}
}

type expectedLog struct {
	direction string
	eventType domain.EventType
}

func fetchLogSequence(t *testing.T, pool *pgxpool.Pool, orderID string) []expectedLog {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT direction, event_type FROM saga_events WHERE order_id=$1 AND component='orchestrator' ORDER BY id",
		orderID)
	if err != nil {
		t.Fatalf("falha ao consultar saga_events: %v", err)
	}
	defer rows.Close()

	var logs []expectedLog
	for rows.Next() {
		var l expectedLog
		if err := rows.Scan(&l.direction, &l.eventType); err != nil {
			t.Fatalf("falha ao ler linha: %v", err)
		}
		logs = append(logs, l)
	}
	return logs
}

func assertSagaFinal(t *testing.T, pool *pgxpool.Pool, orderID string, want domain.OrderStatus) {
	t.Helper()
	repo := infrapostgres.NewSagaRepository(pool)
	saga, err := repo.Load(context.Background(), orderID)
	if err != nil {
		t.Fatalf("falha ao carregar saga %s: %v", orderID, err)
	}
	if saga.Current != want {
		t.Errorf("saga %s: estado final esperado %s, obtido %s", orderID, want, saga.Current)
	}
}

func TestFlowPersistence_Success(t *testing.T) {
	pool := newFlowPool(t)
	cleanFlowTables(t, pool)
	ctx := context.Background()

	orch := orchestrator.New(uow.New(pool), 3)

	orderID := "order-flow-ok"
	if err := orch.StartOrder(ctx, flowEvent(orderID, "e-created", domain.EventOrderCreated, domain.StatusPending, "")); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}

	pay := flowEvent(orderID, "e-pay-1", domain.EventPaymentResult, domain.StatusPaymentApproved, "tx-flow-1")
	if err := orch.HandleResult(ctx, pay); err != nil {
		t.Fatalf("payment falhou: %v", err)
	}
	inv := flowEvent(orderID, "e-inv-1", domain.EventInventoryResult, domain.StatusInventoryReserved, "")
	if err := orch.HandleResult(ctx, inv); err != nil {
		t.Fatalf("inventory falhou: %v", err)
	}
	notif := flowEvent(orderID, "e-notif-1", domain.EventNotificationResult, domain.StatusNotified, "")
	if err := orch.HandleResult(ctx, notif); err != nil {
		t.Fatalf("notification falhou: %v", err)
	}

	assertSagaFinal(t, pool, orderID, domain.StatusCompleted)

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
	got := fetchLogSequence(t, pool, orderID)
	if len(got) != len(want) {
		t.Fatalf("esperados %d registros no journal, obtidos %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("registro %d: esperado %+v, obtido %+v", i, want[i], got[i])
		}
	}
}

func TestFlowPersistence_Compensation(t *testing.T) {
	pool := newFlowPool(t)
	cleanFlowTables(t, pool)
	ctx := context.Background()

	orch := orchestrator.New(uow.New(pool), 3)

	orderID := "order-flow-comp"
	if err := orch.StartOrder(ctx, flowEvent(orderID, "e-created", domain.EventOrderCreated, domain.StatusPending, "")); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}

	pay := flowEvent(orderID, "e-pay-1", domain.EventPaymentResult, domain.StatusPaymentApproved, "tx-flow-comp")
	if err := orch.HandleResult(ctx, pay); err != nil {
		t.Fatalf("payment falhou: %v", err)
	}
	invFail := flowEvent(orderID, "e-inv-fail", domain.EventInventoryResult, domain.StatusFailed, "")
	if err := orch.HandleResult(ctx, invFail); err != nil {
		t.Fatalf("inventory fail deveria acionar compensação: %v", err)
	}
	refund := flowEvent(orderID, "e-refund-1", domain.EventPaymentCompensateResult, domain.StatusPaymentRefunded, "tx-flow-comp")
	if err := orch.HandleResult(ctx, refund); err != nil {
		t.Fatalf("refund falhou: %v", err)
	}

	assertSagaFinal(t, pool, orderID, domain.StatusFailed)

	want := []expectedLog{
		{"IN", domain.EventOrderCreated},
		{"OUT", domain.EventPaymentCommand},
		{"IN", domain.EventPaymentResult},
		{"OUT", domain.EventInventoryCommand},
		{"IN", domain.EventInventoryResult},
		{"OUT", domain.EventPaymentCompensate},
		{"IN", domain.EventPaymentCompensateResult},
		{"OUT", domain.EventOrderFailed},
	}
	got := fetchLogSequence(t, pool, orderID)
	if len(got) != len(want) {
		t.Fatalf("esperados %d registros no journal, obtidos %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("registro %d: esperado %+v, obtido %+v", i, want[i], got[i])
		}
	}
}

func TestFlowPersistence_RetryOnce(t *testing.T) {
	pool := newFlowPool(t)
	cleanFlowTables(t, pool)
	ctx := context.Background()

	orch := orchestrator.New(uow.New(pool), 3)

	orderID := "order-flow-retry"
	if err := orch.StartOrder(ctx, flowEvent(orderID, "e-created", domain.EventOrderCreated, domain.StatusPending, "")); err != nil {
		t.Fatalf("StartOrder falhou: %v", err)
	}

	retry1 := flowEvent(orderID, "e-retry-1", domain.EventPaymentResult, domain.StatusRetrying, "")
	if err := orch.HandleResult(ctx, retry1); err != nil {
		t.Fatalf("primeiro retry falhou: %v", err)
	}
	retry2 := flowEvent(orderID, "e-retry-2", domain.EventPaymentResult, domain.StatusRetrying, "")
	if err := orch.HandleResult(ctx, retry2); err != nil {
		t.Fatalf("segundo retry falhou: %v", err)
	}

	// O retryCount deve estar persistido no banco após os dois RETRYING.
	repo := infrapostgres.NewSagaRepository(pool)
	saga, err := repo.Load(ctx, orderID)
	if err != nil {
		t.Fatalf("falha ao carregar saga: %v", err)
	}
	if saga.RetryCount != 2 {
		t.Errorf("retryCount esperado 2, obtido %d", saga.RetryCount)
	}

	pay := flowEvent(orderID, "e-pay-ok", domain.EventPaymentResult, domain.StatusPaymentApproved, "tx-retry-1")
	if err := orch.HandleResult(ctx, pay); err != nil {
		t.Fatalf("payment final falhou: %v", err)
	}
	inv := flowEvent(orderID, "e-inv-1", domain.EventInventoryResult, domain.StatusInventoryReserved, "")
	if err := orch.HandleResult(ctx, inv); err != nil {
		t.Fatalf("inventory falhou: %v", err)
	}
	notif := flowEvent(orderID, "e-notif-1", domain.EventNotificationResult, domain.StatusNotified, "")
	if err := orch.HandleResult(ctx, notif); err != nil {
		t.Fatalf("notification falhou: %v", err)
	}

	assertSagaFinal(t, pool, orderID, domain.StatusCompleted)

	want := []expectedLog{
		{"IN", domain.EventOrderCreated},
		{"OUT", domain.EventPaymentCommand},
		{"IN", domain.EventPaymentResult},
		{"OUT", domain.EventPaymentCommand},
		{"IN", domain.EventPaymentResult},
		{"OUT", domain.EventPaymentCommand},
		{"IN", domain.EventPaymentResult},
		{"OUT", domain.EventInventoryCommand},
		{"IN", domain.EventInventoryResult},
		{"OUT", domain.EventNotificationCommand},
		{"IN", domain.EventNotificationResult},
		{"OUT", domain.EventOrderCompleted},
	}
	got := fetchLogSequence(t, pool, orderID)
	if len(got) != len(want) {
		t.Fatalf("esperados %d registros no journal, obtidos %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("registro %d: esperado %+v, obtido %+v", i, want[i], got[i])
		}
	}
}
