package postgres_read

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/domain"
)

// OrderViewRepository persiste o read model de pedidos no banco de leitura.
type OrderViewRepository struct {
	pool *pgxpool.Pool
}

// NewOrderViewRepository cria o repositório do read model sobre o pool informado.
func NewOrderViewRepository(pool *pgxpool.Pool) *OrderViewRepository {
	return &OrderViewRepository{pool: pool}
}

// ApplyEvent atualiza order_views a partir de um evento do barramento, fazendo upsert
// da linha do pedido e anexando o evento à timeline (append-only).
func (r *OrderViewRepository) ApplyEvent(ctx context.Context, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("erro ao serializar evento %s: %w", event.EventID, err)
	}

	notificationError := event.Metadata["notification_error"] == "true"
	paymentRefundFailed := event.Metadata["payment_refund_failed"] == "true"

	// Estados terminais são FINAIS: o projector consome vários tópicos com um único
	// consumer group e o Kafka não garante ordem entre tópicos — um evento atrasado
	// (ex.: NOTIFICATION_RESULT chegando depois de ORDER_COMPLETED) não pode regredir
	// o read model de COMPLETED/FAILED. Eventos atrasados seguem entrando na timeline.
	const query = `
INSERT INTO order_views (order_id, current_status, last_event_type, last_event_at, transaction_id, notification_error, payment_refund_failed, timeline, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, jsonb_build_array($8::jsonb), now())
ON CONFLICT (order_id) DO UPDATE SET
	current_status        = CASE WHEN order_views.current_status IN ('COMPLETED', 'FAILED')
	                             THEN order_views.current_status
	                             ELSE EXCLUDED.current_status END,
	last_event_type       = CASE WHEN order_views.current_status IN ('COMPLETED', 'FAILED')
	                             THEN order_views.last_event_type
	                             ELSE EXCLUDED.last_event_type END,
	last_event_at         = CASE WHEN order_views.current_status IN ('COMPLETED', 'FAILED')
	                             THEN order_views.last_event_at
	                             ELSE EXCLUDED.last_event_at END,
	transaction_id        = CASE WHEN EXCLUDED.transaction_id <> '' THEN EXCLUDED.transaction_id ELSE order_views.transaction_id END,
	notification_error    = order_views.notification_error OR EXCLUDED.notification_error,
	payment_refund_failed = order_views.payment_refund_failed OR EXCLUDED.payment_refund_failed,
	timeline              = order_views.timeline || jsonb_build_array($8::jsonb),
	updated_at            = now()`

	_, err = r.pool.Exec(ctx, query,
		event.OrderID,
		string(event.StatusAtual),
		string(event.EventType),
		event.CreatedAt,
		event.TransactionID,
		notificationError,
		paymentRefundFailed,
		payload,
	)
	if err != nil {
		return fmt.Errorf("erro ao aplicar evento %s no read model: %w", event.EventID, err)
	}
	return nil
}

// MarkProcessed registra o event_id como processado; retorna false se já existia (dedup).
func (r *OrderViewRepository) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	const query = `
INSERT INTO processed_events (event_id) VALUES ($1)
ON CONFLICT (event_id) DO NOTHING`

	tag, err := r.pool.Exec(ctx, query, eventID)
	if err != nil {
		return false, fmt.Errorf("erro ao marcar evento %s como processado: %w", eventID, err)
	}
	return tag.RowsAffected() > 0, nil
}
