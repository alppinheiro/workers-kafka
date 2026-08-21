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
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/interfaces"
)

// main sobe o worker de estoque, consumindo comandos do tópico orders.inventory e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: "worker-inventory",
		Topic:   infrakafka.TopicOrderInventory,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewInventorySimulator(0.9)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("worker de estoque: %v", err)
	}
	defer pool.Close()

	eventLog := infrapostgres.NewEventLogRepository(pool)
	useCase := worker.NewInventoryUseCase(gateway, producer, eventLog)

	log.Println("worker de estoque: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-inventory", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de estoque: encerrado com erro: %v", err)
	}
	log.Println("worker de estoque: encerrado")
}
