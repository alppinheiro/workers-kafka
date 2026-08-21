package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

func TestEventLogRepository_AppendPersistsJSONB(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewEventLogRepository(pool)

	entry := application.EventLogEntry{
		OrderID:         "order-it-010",
		SagaID:          "order-it-010",
		EventID:         "evt-it-1",
		EventType:       domain.EventPaymentCommand,
		Component:       "orchestrator",
		Direction:       application.DirectionOut,
		StatusAnterior:  domain.StatusPending,
		StatusAtual:     domain.StatusPaymentPending,
		Payload:         map[string]any{"order_id": "order-it-010", "type": "PAYMENT_COMMAND"},
		RequestPayload:  map[string]any{"op": "process"},
		ResponsePayload: map[string]any{"approved": true},
	}

	if err := repo.Append(ctx, entry); err != nil {
		t.Fatalf("Append falhou: %v", err)
	}

	var payloadJSON, requestJSON, responseJSON []byte
	var direction, component string
	var eventType string
	err := pool.QueryRow(ctx,
		"SELECT event_type, component, direction, payload, request_payload, response_payload FROM saga_events WHERE event_id=$1 AND component=$2",
		entry.EventID, entry.Component).Scan(&eventType, &component, &direction, &payloadJSON, &requestJSON, &responseJSON)
	if err != nil {
		t.Fatalf("falha ao consultar linha persistida: %v", err)
	}

	if eventType != string(domain.EventPaymentCommand) || component != "orchestrator" || direction != application.DirectionOut {
		t.Errorf("colunas básicas incorretas: type=%s component=%s direction=%s", eventType, component, direction)
	}
	if payloadJSON == nil || requestJSON == nil || responseJSON == nil {
		t.Fatal("payloads JSONB não foram persistidos")
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadJSON, &payload); err != nil || payload["type"] != "PAYMENT_COMMAND" {
		t.Errorf("payload JSONB inválido: %s (%v)", payloadJSON, err)
	}
}

func TestEventLogRepository_AppendIsIdempotentByDirection(t *testing.T) {
	pool := newTestPool(t)
	cleanSagas(t, pool)
	ctx := context.Background()
	repo := NewEventLogRepository(pool)

	// Mesmo event_id + component, direções diferentes: devem coexistir (UNIQUE triplo).
	directions := []string{application.DirectionIn, application.DirectionGatewayRequest, application.DirectionGatewayResponse}
	for _, d := range directions {
		entry := application.EventLogEntry{
			OrderID:   "order-it-011",
			SagaID:    "order-it-011",
			EventID:   "evt-it-2",
			EventType: domain.EventPaymentCommand,
			Component: "worker-payment",
			Direction: d,
		}
		if err := repo.Append(ctx, entry); err != nil {
			t.Fatalf("Append (%s) falhou: %v", d, err)
		}
	}

	// Redelivery: repetir a mesma (event_id, component, direction) não deve duplicar.
	dup := application.EventLogEntry{
		OrderID:   "order-it-011",
		SagaID:    "order-it-011",
		EventID:   "evt-it-2",
		EventType: domain.EventPaymentCommand,
		Component: "worker-payment",
		Direction: application.DirectionIn,
	}
	if err := repo.Append(ctx, dup); err != nil {
		t.Fatalf("Append duplicado falhou: %v", err)
	}

	var total int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM saga_events WHERE event_id=$1 AND component=$2", "evt-it-2", "worker-payment").Scan(&total); err != nil {
		t.Fatalf("falha ao contar linhas: %v", err)
	}
	if total != len(directions) {
		t.Errorf("esperado %d linhas (1 por direção), obtidas %d", len(directions), total)
	}
}
