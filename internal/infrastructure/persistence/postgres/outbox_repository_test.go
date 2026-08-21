package postgres

import (
	"context"
	"testing"
	"time"
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

func TestOutboxRepository_MarkPublishedBatch(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	// Sem ids: no-op (não deve dar erro).
	if err := repo.MarkPublishedBatch(ctx, nil); err != nil {
		t.Fatalf("MarkPublishedBatch com lote vazio falhou: %v", err)
	}

	for i := 1; i <= 3; i++ {
		entry := OutboxEntry{
			EventID: "evt-ob-batch-" + string(rune('a'+i-1)),
			Topic:   "orders.payment",
			Key:     "order-batch",
			Payload: []byte(`{}`),
		}
		if err := repo.Append(ctx, entry); err != nil {
			t.Fatalf("Append falhou: %v", err)
		}
	}

	pending, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending falhou: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("esperado 3 pendentes, obtidos %d", len(pending))
	}

	ids := make([]int64, len(pending))
	for i, e := range pending {
		ids[i] = e.ID
	}
	if err := repo.MarkPublishedBatch(ctx, ids); err != nil {
		t.Fatalf("MarkPublishedBatch falhou: %v", err)
	}

	remaining, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending após batch falhou: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("esperado outbox vazia após MarkPublishedBatch, obtidos %d", len(remaining))
	}
}

func TestOutboxRepository_PurgePublished(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	for i := 1; i <= 2; i++ {
		entry := OutboxEntry{
			EventID: "evt-ob-purge-" + string(rune('a'+i-1)),
			Topic:   "orders.payment",
			Key:     "order-purge",
			Payload: []byte(`{}`),
		}
		if err := repo.Append(ctx, entry); err != nil {
			t.Fatalf("Append falhou: %v", err)
		}
	}

	pending, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending falhou: %v", err)
	}
	ids := make([]int64, len(pending))
	for i, e := range pending {
		ids[i] = e.ID
	}
	if err := repo.MarkPublishedBatch(ctx, ids); err != nil {
		t.Fatalf("MarkPublishedBatch falhou: %v", err)
	}

	// Simula eventos publicados há muito tempo (7 dias + 1s) e roda a purga.
	olderThan := time.Hour
	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = published_at - interval '8 days'`); err != nil {
		t.Fatalf("ajuste de published_at falhou: %v", err)
	}
	removed, err := repo.PurgePublished(ctx, olderThan)
	if err != nil {
		t.Fatalf("PurgePublished falhou: %v", err)
	}
	if removed != 2 {
		t.Errorf("esperado 2 linhas purgadas, removidas %d", removed)
	}

	// O DELETE de uma segunda vez não remove nada (retornando 0, sem erro).
	removed, err = repo.PurgePublished(ctx, olderThan)
	if err != nil {
		t.Fatalf("segunda PurgePublished falhou: %v", err)
	}
	if removed != 0 {
		t.Errorf("esperado 0 na segunda purga, removidas %d", removed)
	}
}
