package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/application"
)

// EventLogRepository persiste o journal de eventos (append-only) no banco de escrita.
type EventLogRepository struct {
	db DBTX
}

// NewEventLogRepository cria o repositório do journal sobre o pool informado.
func NewEventLogRepository(pool *pgxpool.Pool) *EventLogRepository {
	return &EventLogRepository{db: pool}
}

// NewEventLogRepositoryTx cria o repositório do journal vinculado a uma transação: as
// gravações executam dentro do pgx.Tx, junto com estado e outbox (atomicidade).
func NewEventLogRepositoryTx(tx pgx.Tx) *EventLogRepository {
	return &EventLogRepository{db: tx}
}

// Append insere uma linha no journal. A gravação é idempotente por (event_id, component,
// direction): em caso de redelivery, o conflito de UNIQUE é ignorado (ON CONFLICT DO NOTHING).
func (r *EventLogRepository) Append(ctx context.Context, entry application.EventLogEntry) error {
	payloadJSON, err := marshalJSON(entry.Payload)
	if err != nil {
		return fmt.Errorf("erro ao serializar payload do evento %s: %w", entry.EventID, err)
	}
	requestJSON, err := marshalJSON(entry.RequestPayload)
	if err != nil {
		return fmt.Errorf("erro ao serializar request_payload do evento %s: %w", entry.EventID, err)
	}
	responseJSON, err := marshalJSON(entry.ResponsePayload)
	if err != nil {
		return fmt.Errorf("erro ao serializar response_payload do evento %s: %w", entry.EventID, err)
	}

	const query = `
INSERT INTO saga_events (order_id, saga_id, event_id, event_type, component, direction, status_anterior, status_atual, payload, request_payload, response_payload)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (event_id, component, direction) DO NOTHING`

	_, err = r.db.Exec(ctx, query,
		entry.OrderID,
		entry.SagaID,
		entry.EventID,
		string(entry.EventType),
		entry.Component,
		entry.Direction,
		string(entry.StatusAnterior),
		string(entry.StatusAtual),
		payloadJSON,
		requestJSON,
		responseJSON,
	)
	if err != nil {
		return fmt.Errorf("erro ao gravar evento %s no journal: %w", entry.EventID, err)
	}
	return nil
}

// Has informa se um evento já foi registrado por um componente (idempotência).
func (r *EventLogRepository) Has(ctx context.Context, eventID string, component string) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM saga_events WHERE event_id = $1 AND component = $2)`

	var exists bool
	if err := r.db.QueryRow(ctx, query, eventID, component).Scan(&exists); err != nil {
		return false, fmt.Errorf("erro ao verificar evento %s no journal: %w", eventID, err)
	}
	return exists, nil
}

func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
