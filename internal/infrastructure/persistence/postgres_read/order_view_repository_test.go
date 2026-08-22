package postgres_read

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/domain"
)

func newTestReadPool(t *testing.T) *pgxpool.Pool {
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

func cleanReadTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"processed_events", "order_views"} {
		if _, err := pool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("falha ao limpar %s: %v", table, err)
		}
	}
}

func sampleEvent(orderID string, eventType domain.EventType, status domain.OrderStatus) domain.Event {
	return domain.Event{
		EventID:       "evt-read-" + orderID,
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   status,
		EventType:     eventType,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}
}

func TestOrderViewRepository_ApplyEventCreatesAndUpserts(t *testing.T) {
	pool := newTestReadPool(t)
	cleanReadTables(t, pool)
	ctx := context.Background()
	repo := NewOrderViewRepository(pool)

	first := sampleEvent("order-read-001", domain.EventPaymentResult, domain.StatusPaymentApproved)
	first.TransactionID = "tx-read-1"
	if err := repo.ApplyEvent(ctx, first); err != nil {
		t.Fatalf("primeiro ApplyEvent falhou: %v", err)
	}

	second := sampleEvent("order-read-001", domain.EventInventoryResult, domain.StatusInventoryReserved)
	if err := repo.ApplyEvent(ctx, second); err != nil {
		t.Fatalf("segundo ApplyEvent falhou: %v", err)
	}

	var currentStatus, transactionID string
	var timelineLen int
	err := pool.QueryRow(ctx,
		"SELECT current_status, transaction_id, jsonb_array_length(timeline) FROM order_views WHERE order_id=$1",
		"order-read-001").Scan(&currentStatus, &transactionID, &timelineLen)
	if err != nil {
		t.Fatalf("falha ao consultar order_views: %v", err)
	}

	if currentStatus != string(domain.StatusInventoryReserved) {
		t.Errorf("status esperado %s, obtido %s", domain.StatusInventoryReserved, currentStatus)
	}
	if transactionID != "tx-read-1" {
		t.Errorf("transaction_id deveria persistir após o primeiro evento, obtido %s", transactionID)
	}
	if timelineLen != 2 {
		t.Errorf("timeline esperada com 2 eventos, obtida com %d", timelineLen)
	}
}

func TestOrderViewRepository_MarkProcessedDedup(t *testing.T) {
	pool := newTestReadPool(t)
	cleanReadTables(t, pool)
	ctx := context.Background()
	repo := NewOrderViewRepository(pool)

	first, err := repo.MarkProcessed(ctx, "evt-dup-1")
	if err != nil {
		t.Fatalf("primeiro MarkProcessed falhou: %v", err)
	}
	second, err := repo.MarkProcessed(ctx, "evt-dup-1")
	if err != nil {
		t.Fatalf("segundo MarkProcessed falhou: %v", err)
	}

	if !first {
		t.Error("primeira chamada deveria retornar true (evento novo)")
	}
	if second {
		t.Error("segunda chamada deveria retornar false (evento já processado)")
	}
}

// TestOrderViewRepository_TerminalStateIsFinal prova a correção da projeção quando o
// projector consome eventos de tópicos diferentes fora de ordem (o Kafka não garante
// ordem entre tópicos): um evento atrasado (NOTIFICATION_RESULT) que chega DEPOIS do
// terminal (ORDER_COMPLETED) não pode regredir o read model — só entra na timeline.
func TestOrderViewRepository_TerminalStateIsFinal(t *testing.T) {
	pool := newTestReadPool(t)
	cleanReadTables(t, pool)
	ctx := context.Background()
	repo := NewOrderViewRepository(pool)
	orderID := "order-read-terminal"

	// Sequência REAL de consumo (fora de ordem): NOTIFICATION_RESULT chega atrasado,
	// depois que o ORDER_COMPLETED já foi aplicado.
	notif := sampleEvent(orderID, domain.EventNotificationResult, domain.StatusNotified)
	notif.EventID = "evt-notif"
	if err := repo.ApplyEvent(ctx, notif); err != nil {
		t.Fatalf("aplicação de NOTIFICATION_RESULT falhou: %v", err)
	}

	completed := sampleEvent(orderID, domain.EventOrderCompleted, domain.StatusCompleted)
	completed.EventID = "evt-completed"
	if err := repo.ApplyEvent(ctx, completed); err != nil {
		t.Fatalf("aplicação de ORDER_COMPLETED falhou: %v", err)
	}

	// Redelivery/fora de ordem: NOTIFICATION_RESULT reaparece após o terminal.
	stale := sampleEvent(orderID, domain.EventNotificationResult, domain.StatusNotified)
	stale.EventID = "evt-notif-dup"
	if err := repo.ApplyEvent(ctx, stale); err != nil {
		t.Fatalf("aplicação do evento atrasado falhou: %v", err)
	}

	var currentStatus, lastEventType string
	var timelineLen int
	err := pool.QueryRow(ctx,
		"SELECT current_status, last_event_type, jsonb_array_length(timeline) FROM order_views WHERE order_id=$1",
		orderID).Scan(&currentStatus, &lastEventType, &timelineLen)
	if err != nil {
		t.Fatalf("falha ao consultar order_views: %v", err)
	}

	if currentStatus != string(domain.StatusCompleted) {
		t.Errorf("status terminal deveria ser final: esperado %s, obtido %s (regressão!)", domain.StatusCompleted, currentStatus)
	}
	if lastEventType != string(domain.EventOrderCompleted) {
		t.Errorf("last_event_type deveria permanecer %s, obtido %s", domain.EventOrderCompleted, lastEventType)
	}
	// O evento atrasado segue registrado na timeline (rastreabilidade), sem regredir o status.
	if timelineLen != 3 {
		t.Errorf("timeline esperada com 3 eventos, obtida com %d", timelineLen)
	}
}
