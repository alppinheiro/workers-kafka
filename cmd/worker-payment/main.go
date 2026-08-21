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

// main sobe o worker de pagamento, consumindo comandos do tópico orders.payment e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: "worker-payment",
		Topic:   infrakafka.TopicOrderPayment,
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
	useCase := worker.NewPaymentUseCase(gateway, producer, eventLog)

	log.Println("worker de pagamento: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-payment", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de pagamento: encerrado com erro: %v", err)
	}
	log.Println("worker de pagamento: encerrado")
}
