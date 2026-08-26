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
VALUES (@order_id, @saga_id, @event_id, @event_type, @component, @direction, @status_anterior, @status_atual, @payload, @request_payload, @response_payload)
ON CONFLICT (event_id, component, direction) DO NOTHING`

	// Named args (review 3.4): com 11 colunas, a ordem posicional ($1..$11) era frágil
	// para manutenção — o nome deixa a query autodocumentada e a ordem irrelevante.
	_, err = r.db.Exec(ctx, query, pgx.NamedArgs{
		"order_id":         entry.OrderID,
		"saga_id":          entry.SagaID,
		"event_id":         entry.EventID,
		"event_type":       string(entry.EventType),
		"component":        entry.Component,
		"direction":        entry.Direction,
		"status_anterior":  string(entry.StatusAnterior),
		"status_atual":     string(entry.StatusAtual),
		"payload":          payloadJSON,
		"request_payload":  requestJSON,
		"response_payload": responseJSON,
	})
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
