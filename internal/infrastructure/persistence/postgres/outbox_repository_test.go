package postgres

import (
	"context"
	"testing"
)

func TestOutboxRepository_AppendFetchAndMarkPublished(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	entry := OutboxEntry{EventID: "evt-ob-1", Topic: "orders.payment", Key: "order-1", Payload: []byte(`{"order_id":"order-1"}`)}
	if err := repo.Append(ctx, entry); err != nil {
		t.Fatalf("primeiro Append falhou: %v", err)
	}

	// Redelivery/duplicado: o INSERT é idempotente por event_id.
	if err := repo.Append(ctx, entry); err != nil {
		t.Fatalf("Append duplicado falhou: %v", err)
	}

	pending, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending falhou: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("esperado 1 evento pendente, obtidos %d", len(pending))
	}
	if pending[0].Topic != "orders.payment" || pending[0].Key != "order-1" {
		t.Errorf("outbox mal persistida: %+v", pending[0])
	}

	if err := repo.MarkPublished(ctx, pending[0].ID); err != nil {
		t.Fatalf("MarkPublished falhou: %v", err)
	}

	remaining, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("segundo FetchPending falhou: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("esperado outbox vazia após publicação, obtidos %d", len(remaining))
	}
}

func TestOutboxRepository_FetchPending_Limit(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	for i := 1; i <= 3; i++ {
		entry := OutboxEntry{
			EventID: "evt-ob-limit",
			Topic:   "orders.payment",
			Key:     "order-limit",
			Payload: []byte(`{}`),
		}
		// event_id é único; para criar 3 linhas distintas, usar event_ids distintos.
		entry.EventID = "evt-ob-limit-" + string(rune('a'+i-1))
		if err := repo.Append(ctx, entry); err != nil {
			t.Fatalf("Append falhou: %v", err)
		}
	}

	pending, err := repo.FetchPending(ctx, 2)
	if err != nil {
		t.Fatalf("FetchPending falhou: %v", err)
	}
	if len(pending) != 2 {
		t.Errorf("esperado limite de 2, obtidos %d", len(pending))
	}
}
