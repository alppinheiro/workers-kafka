package interfaces

import (
	"context"
	"fmt"
	"log/slog"
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
		slog.InfoContext(ctx, "evento recebido",
			"component", component, "phase", "received",
			"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
			"transaction_id", event.TransactionID, "type", event.EventType,
			"status_previous", event.StatusAnterior, "status_current", event.StatusAtual,
			"schema_version", event.SchemaVersion, "metadata", formatMetadata(event.Metadata),
		)

		if err := next(ctx, event); err != nil {
			slog.ErrorContext(ctx, "evento falhou",
				"component", component, "phase", "failed",
				"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
				"transaction_id", event.TransactionID, "type", event.EventType,
				"status_previous", event.StatusAnterior, "status_current", event.StatusAtual,
				"duration", time.Since(startedAt), "error", err,
			)
			return err
		}

		slog.InfoContext(ctx, "evento processado",
			"component", component, "phase", "processed",
			"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
			"transaction_id", event.TransactionID, "type", event.EventType,
			"status_previous", event.StatusAnterior, "status_current", event.StatusAtual,
			"duration", time.Since(startedAt),
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
