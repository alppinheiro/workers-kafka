package main

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"workers-kafka/internal/application/worker"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/infrastructure/telemetry"
)

// main publica o evento inicial de um novo pedido (PENDING), disparando o início da saga.
// Uso: go run ./cmd/create-order <order_id>
func main() {
	logging.Setup("create-order")
	if len(os.Args) < 2 {
		slog.Error("uso: create-order <order_id>")
		os.Exit(1)
	}
	orderID := os.Args[1]

	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers, "create-order")
	defer func() { _ = producer.Close() }()

	shutdown, err := telemetry.Init("create-order")
	if err != nil {
		slog.Error("falha ao inicializar telemetria", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdown(context.Background()) }()

	coordinator := worker.NewCoordinator(producer)

	// Root span: o traceparent é injetado no header do ORDER_CREATED pelo producer.
	ctx, span := otel.Tracer("create-order").Start(context.Background(), "create-order",
		trace.WithAttributes(attribute.String("order_id", orderID)))
	defer span.End()

	if err := coordinator.CreateOrder(ctx, orderID); err != nil {
		span.RecordError(err)
		slog.Error("erro ao criar pedido", "order_id", orderID, "error", err)
		os.Exit(1)
	}

	slog.Info("pedido criado", "order_id", orderID, "status", "PENDING")
}
