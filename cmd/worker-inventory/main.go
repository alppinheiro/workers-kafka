package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/worker"
	"workers-kafka/internal/infrastructure/external"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/outbox"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/interfaces"
)

// main sobe o worker de estoque, consumindo comandos do tópico orders.inventory e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "worker-inventory",
		ServiceName: "worker-inventory",
		Workers:     infrakafka.WorkersFromEnv(),
		Topic:       infrakafka.TopicOrderInventory,
		DLQWriter:   dlq,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewInventorySimulator(0.9)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("worker-inventory")
	if err != nil {
		log.Fatalf("worker de estoque: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("worker de estoque: %v", err)
	}
	defer pool.Close()

	eventLog := infrapostgres.NewEventLogRepository(pool)
	publisher := outbox.NewPublisher(infrapostgres.NewOutboxRepository(pool))
	useCase := worker.NewInventoryUseCase(gateway, publisher, eventLog)

	log.Println("worker de estoque: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-inventory", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de estoque: encerrado com erro: %v", err)
	}
	log.Println("worker de estoque: encerrado")
}
