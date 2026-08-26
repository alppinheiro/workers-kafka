package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/worker"
	"workers-kafka/internal/infrastructure/external"
	"workers-kafka/internal/infrastructure/health"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/infrastructure/uow"
	"workers-kafka/internal/interfaces"
)

// main sobe o worker de estoque, consumindo comandos do tópico orders.inventory.cmd e publicando o resultado.
func main() {
	logging.Setup("worker-inventory")
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "worker-inventory",
		ServiceName: "worker-inventory",
		Workers:     infrakafka.WorkersFromEnv(),
		Topic:       infrakafka.TopicInventoryCommand,
		DLQWriter:   dlq,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewInventorySimulator(0.9)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("worker-inventory")
	if err != nil {
		slog.Error("falha ao inicializar telemetria", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		slog.Error("falha ao conectar no postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	metrics.ServeWithChecks(":9103", health.Postgres(pool), health.Kafka(brokers))

	useCase := worker.NewInventoryUseCase(external.InventoryGatewayFromEnv(gateway), uow.New(pool))

	slog.Info("worker de estoque: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-inventory", useCase.Handle)); err != nil && ctx.Err() == nil {
		slog.Error("worker de estoque encerrou com erro", "error", err)
		os.Exit(1)
	}
	slog.Info("worker de estoque: encerrado")
}
