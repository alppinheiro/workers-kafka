package postgres_read_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application/projector"
	"workers-kafka/internal/domain"
	"workers-kafka/internal/infrastructure/persistence/postgres_read"
)

func newProjectorPool(t *testing.T) *pgxpool.Pool {
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

func cleanProjectorTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"processed_events", "order_views"} {
		if _, err := pool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("falha ao limpar %s: %v", table, err)
		}
	}
}

func projectorEvent(orderID, eventID string, eventType domain.EventType, status domain.OrderStatus) domain.Event {
	return domain.Event{
		EventID:       eventID,
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   status,
		EventType:     eventType,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}
}

// TestProjectorFlow_Persistence comprova que o read model é montado de forma correta e
// idempotente a partir dos eventos do barramento, com timeline completa em ordem.
func TestProjectorFlow_Persistence(t *testing.T) {
	pool := newProjectorPool(t)
	cleanProjectorTables(t, pool)
	ctx := context.Background()

	views := postgres_read.NewOrderViewRepository(pool)
	proj := projector.New(views)

	orderID := "order-proj-ok"
	events := []domain.Event{
		projectorEvent(orderID, "e-1", domain.EventOrderCreated, domain.StatusPending),
		projectorEvent(orderID, "e-2", domain.EventPaymentResult, domain.StatusPaymentApproved),
		projectorEvent(orderID, "e-3", domain.EventInventoryResult, domain.StatusInventoryReserved),
		projectorEvent(orderID, "e-4", domain.EventNotificationResult, domain.StatusNotified),
		projectorEvent(orderID, "e-5", domain.EventOrderCompleted, domain.StatusCompleted),
	}
	for _, evt := range events {
		if err := proj.HandleEvent(ctx, evt); err != nil {
			t.Fatalf("HandleEvent (%s) falhou: %v", evt.EventType, err)
		}
	}

	var currentStatus string
	var timelineLen int
	err := pool.QueryRow(ctx,
		"SELECT current_status, jsonb_array_length(timeline) FROM order_views WHERE order_id=$1",
		orderID).Scan(&currentStatus, &timelineLen)
	if err != nil {
		t.Fatalf("falha ao consultar order_views: %v", err)
	}
	if currentStatus != string(domain.StatusCompleted) {
		t.Errorf("status final esperado %s, obtido %s", domain.StatusCompleted, currentStatus)
	}
	if timelineLen != len(events) {
		t.Errorf("timeline esperada com %d eventos, obtida com %d", len(events), timelineLen)
	}

	// Redelivery: reprocessar o mesmo evento não deve alterar o read model.
	if err := proj.HandleEvent(ctx, events[2]); err != nil {
		t.Fatalf("redelivery falhou: %v", err)
	}
	var after int
	if err := pool.QueryRow(ctx,
		"SELECT jsonb_array_length(timeline) FROM order_views WHERE order_id=$1", orderID).Scan(&after); err != nil {
		t.Fatalf("falha ao consultar timeline após redelivery: %v", err)
	}
	if after != len(events) {
		t.Errorf("redelivery deveria ser ignorado (dedup); timeline=%d", after)
	}
}
