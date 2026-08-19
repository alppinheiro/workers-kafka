package interfaces

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// WithLogging decora um EventHandler com rastreabilidade básica de entrada e saída, sem acoplar os
// workers e o orquestrador a uma dependência de observabilidade concreta.
func WithLogging(component string, next application.EventHandler) application.EventHandler {
	return func(ctx context.Context, event domain.Event) error {
		startedAt := time.Now()
		log.Printf("component=%s phase=received event_id=%s order_id=%s saga_id=%s type=%s status_previous=%s status_current=%s schema_version=%d metadata=%s",
			component,
			event.EventID,
			event.OrderID,
			event.SagaID,
			event.EventType,
			event.StatusAnterior,
			event.StatusAtual,
			event.SchemaVersion,
			formatMetadata(event.Metadata),
		)

		if err := next(ctx, event); err != nil {
			log.Printf("component=%s phase=failed event_id=%s order_id=%s saga_id=%s type=%s status_previous=%s status_current=%s duration=%s error=%v",
				component,
				event.EventID,
				event.OrderID,
				event.SagaID,
				event.EventType,
				event.StatusAnterior,
				event.StatusAtual,
				time.Since(startedAt),
				err,
			)
			return err
		}

		log.Printf("component=%s phase=processed event_id=%s order_id=%s saga_id=%s type=%s status_previous=%s status_current=%s duration=%s",
			component,
			event.EventID,
			event.OrderID,
			event.SagaID,
			event.EventType,
			event.StatusAnterior,
			event.StatusAtual,
			time.Since(startedAt),
		)

		return nil
	}
}

func formatMetadata(metadata map[string]string) string {
	if len(metadata) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, metadata[key]))
	}

	return strings.Join(parts, ",")
}
