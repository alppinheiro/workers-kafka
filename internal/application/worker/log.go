package worker

import (
	"context"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// Nomes de componentes usados no journal de eventos.
const (
	componentPayment      = "worker-payment"
	componentInventory    = "worker-inventory"
	componentNotification = "worker-notification"
)

// appendLog registra a visão do worker sobre um evento no journal (append-only). Se o
// eventLog for nil, a gravação é ignorada (útil em cenários sem persistência).
func appendLog(ctx context.Context, eventLog application.EventLogRepository, component string, event domain.Event, direction string, requestPayload, responsePayload any) error {
	if eventLog == nil {
		return nil
	}
	return eventLog.Append(ctx, application.EventLogEntry{
		OrderID:         event.OrderID,
		SagaID:          event.SagaID,
		EventID:         event.EventID,
		EventType:       event.EventType,
		Component:       component,
		Direction:       direction,
		StatusAnterior:  event.StatusAnterior,
		StatusAtual:     event.StatusAtual,
		Payload:         event,
		RequestPayload:  requestPayload,
		ResponsePayload: responsePayload,
	})
}
