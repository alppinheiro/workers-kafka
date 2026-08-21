package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/projector"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	infrapostgresread "workers-kafka/internal/infrastructure/persistence/postgres_read"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/interfaces"
)

// main sobe o projector: consome os cinco tópicos do barramento e mantém o read model
// (order_views) no banco de leitura, com dedup por event_id.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "projector",
		ServiceName: "projector",
		Topics: []string{
			infrakafka.TopicOrderCreated,
			infrakafka.TopicOrderPayment,
			infrakafka.TopicOrderInventory,
			infrakafka.TopicOrderNotification,
			infrakafka.TopicOrderStatus,
		},
		DLQWriter: dlq,
	})
	defer func() { _ = consumer.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("projector")
	if err != nil {
		log.Fatalf("projector: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgresread.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("projector: %v", err)
	}
	defer pool.Close()

	views := infrapostgresread.NewOrderViewRepository(pool)
	proj := projector.New(views)

	log.Println("projector: aguardando eventos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("projector", proj.HandleEvent)); err != nil && ctx.Err() == nil {
		log.Fatalf("projector: encerrado com erro: %v", err)
	}
	log.Println("projector: encerrado")
}
