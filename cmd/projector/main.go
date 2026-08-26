package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/projector"
	"workers-kafka/internal/infrastructure/health"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	infrapostgresread "workers-kafka/internal/infrastructure/persistence/postgres_read"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/interfaces"
)

// main sobe o projector: consome todos os tópicos do barramento e mantém o read model
// (order_views) no banco de leitura, com dedup por event_id.
func main() {
	logging.Setup("projector")
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "projector",
		ServiceName: "projector",
		Workers:     infrakafka.WorkersFromEnv(),
		Topics:      infrakafka.FlowTopics(),
		DLQWriter:   dlq,
	})
	defer func() { _ = consumer.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("projector")
	if err != nil {
		slog.Error("falha ao inicializar telemetria", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgresread.DatabaseURLFromEnv())
	if err != nil {
		slog.Error("falha ao conectar no postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	metrics.ServeWithChecks(":9105", health.Postgres(pool), health.Kafka(brokers))

	views := infrapostgresread.NewOrderViewRepository(pool)
	proj := projector.New(views)

	slog.Info("projector: aguardando eventos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("projector", proj.HandleEvent)); err != nil && ctx.Err() == nil {
		slog.Error("projector encerrou com erro", "error", err)
		os.Exit(1)
	}
	slog.Info("projector: encerrado")
}
