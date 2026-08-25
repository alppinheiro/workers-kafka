package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/orchestrator"
	"workers-kafka/internal/infrastructure/health"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/infrastructure/uow"
	"workers-kafka/internal/interfaces"
)

// main sobe o orquestrador da saga: um único consumer acompanha os três tópicos de
// resultado (pagamento, estoque e notificação) sem depender de goroutines adicionais.
// O estado da saga é persistido em PostgreSQL e as transições são registradas no journal.
func main() {
	logging.Setup("orchestrator")
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "orchestrator",
		ServiceName: "orchestrator",
		Workers:     infrakafka.WorkersFromEnv(),
		Topics: []string{
			infrakafka.TopicOrderCreated,
			infrakafka.TopicOrderPayment,
			infrakafka.TopicOrderInventory,
			infrakafka.TopicOrderNotification,
		},
		DLQWriter: dlq,
	})
	defer func() { _ = consumer.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("orchestrator")
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

	metrics.ServeWithChecks(":9101", health.Postgres(pool), health.Kafka(brokers))

	orch := orchestrator.New(uow.New(pool), 3)

	slog.Info("orquestrador: aguardando eventos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("orchestrator", orch.HandleEvent)); err != nil && ctx.Err() == nil {
		slog.Error("orquestrador encerrou com erro", "error", err)
		os.Exit(1)
	}
	slog.Info("orquestrador: encerrado")
}
