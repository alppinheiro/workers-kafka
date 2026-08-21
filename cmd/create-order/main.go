package main

import (
	"context"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"workers-kafka/internal/application/worker"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/telemetry"
)

// main publica o evento inicial de um novo pedido (PENDING), disparando o início da saga.
// Uso: go run ./cmd/create-order <order_id>
func main() {
	if len(os.Args) < 2 {
		log.Fatal("uso: create-order <order_id>")
	}
	orderID := os.Args[1]

	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers, "create-order")
	defer func() { _ = producer.Close() }()

	shutdown, err := telemetry.Init("create-order")
	if err != nil {
		log.Fatalf("create-order: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	coordinator := worker.NewCoordinator(producer)

	// Root span: o traceparent é injetado no header do ORDER_CREATED pelo producer.
	ctx, span := otel.Tracer("create-order").Start(context.Background(), "create-order",
		trace.WithAttributes(attribute.String("order_id", orderID)))
	defer span.End()

	if err := coordinator.CreateOrder(ctx, orderID); err != nil {
		span.RecordError(err)
		log.Fatalf("erro ao criar pedido: %v", err)
	}

	log.Printf("pedido %s criado com status PENDING", orderID)
}
