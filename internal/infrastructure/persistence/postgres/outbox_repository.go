package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxEntry representa um evento registrado na outbox aguardando publicação no Kafka.
type OutboxEntry struct {
	ID          int64
	EventID     string
	Topic       string
	Key         string
	Payload     []byte
	Traceparent string
}

// OutboxRepository persiste eventos a publicar no Kafka (Outbox Pattern).
type OutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository cria o repositório da outbox sobre o pool informado.
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

// Append insere um evento na outbox. A gravação é idempotente por event_id (UNIQUE).
func (r *OutboxRepository) Append(ctx context.Context, entry OutboxEntry) error {
	const query = `
INSERT INTO outbox (event_id, topic, key, payload, traceparent)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (event_id) DO NOTHING`

	_, err := r.pool.Exec(ctx, query, entry.EventID, entry.Topic, entry.Key, entry.Payload, entry.Traceparent)
	if err != nil {
		return fmt.Errorf("erro ao gravar evento %s na outbox: %w", entry.EventID, err)
	}
	return nil
}

// FetchPending retorna até limit eventos ainda não publicados, em ordem de criação.
// Mantido para leitura simples (sem claims); use ClaimPending em relays com escala horizontal.
func (r *OutboxRepository) FetchPending(ctx context.Context, limit int) ([]OutboxEntry, error) {
	const query = `
SELECT id, event_id, topic, key, payload, COALESCE(traceparent, '') FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1`

	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar outbox pendente: %w", err)
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.EventID, &e.Topic, &e.Key, &e.Payload, &e.Traceparent); err != nil {
			return nil, fmt.Errorf("erro ao ler outbox pendente: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// ClaimPending reserva (claims) até limit eventos para este processador, usando
// FOR UPDATE SKIP LOCKED: com múltiplas instâncias do relay, cada linha é processada
// por exatamente um relay. Linhas órfãs (claim antigo sem publicação) são reclamadas
// após claimTimeout.
func (r *OutboxRepository) ClaimPending(ctx context.Context, limit int, claimTimeout time.Duration) ([]OutboxEntry, error) {
	const query = `
UPDATE outbox
SET claimed_at = now()
WHERE id IN (
    SELECT id FROM outbox
    WHERE published_at IS NULL
      AND (claimed_at IS NULL OR claimed_at < now() - $2::interval)
    ORDER BY id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, event_id, topic, key, payload, COALESCE(traceparent, '')`

	rows, err := r.pool.Query(ctx, query, limit, claimTimeout.String())
	if err != nil {
		return nil, fmt.Errorf("erro ao reivindicar outbox pendente: %w", err)
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.EventID, &e.Topic, &e.Key, &e.Payload, &e.Traceparent); err != nil {
			return nil, fmt.Errorf("erro ao ler outbox reivindicada: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// CountPending retorna quantas linhas ainda não publicadas existem na outbox.
func (r *OutboxRepository) CountPending(ctx context.Context) (int, error) {
	const query = `SELECT count(*) FROM outbox WHERE published_at IS NULL`

	var n int
	if err := r.pool.QueryRow(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("erro ao contar outbox pendente: %w", err)
	}
	return n, nil
}

// MarkPublished marca um evento como publicado.
func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	const query = `UPDATE outbox SET published_at = now() WHERE id = $1`

	if _, err := r.pool.Exec(ctx, query, id); err != nil {
		return fmt.Errorf("erro ao marcar outbox %d como publicado: %w", id, err)
	}
	return nil
}

// MarkPublishedBatch marca um lote de eventos como publicado em uma única instrução
// (1 round-trip), em vez de um UPDATE por evento. Usado pelo outbox-relay em alta vazão.
func (r *OutboxRepository) MarkPublishedBatch(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	const query = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`

	if _, err := r.pool.Exec(ctx, query, ids); err != nil {
		return fmt.Errorf("erro ao marcar lote da outbox como publicado: %w", err)
	}
	return nil
}

// PurgePublished remove eventos já publicados há mais de olderThan (retenção da outbox).
// Sem isso a tabela cresce indefinidamente, degradando consultas e o CountPending.
func (r *OutboxRepository) PurgePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	const query = `DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - $1::interval`

	tag, err := r.pool.Exec(ctx, query, olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("erro ao purgar outbox publicada: %w", err)
	}
	return tag.RowsAffected(), nil
}
