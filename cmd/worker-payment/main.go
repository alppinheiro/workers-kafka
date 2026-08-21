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
	"workers-kafka/internal/interfaces"
)

// main sobe o worker de pagamento, consumindo comandos do tópico orders.payment e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:   brokers,
		GroupID:   "worker-payment",
		Topic:     infrakafka.TopicOrderPayment,
		DLQWriter: dlq,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewPaymentSimulator(0.85)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("worker de pagamento: %v", err)
	}
	defer pool.Close()

	eventLog := infrapostgres.NewEventLogRepository(pool)
	publisher := outbox.NewPublisher(infrapostgres.NewOutboxRepository(pool))
	useCase := worker.NewPaymentUseCase(gateway, publisher, eventLog)

	log.Println("worker de pagamento: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-payment", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de pagamento: encerrado com erro: %v", err)
	}
	log.Println("worker de pagamento: encerrado")
}
