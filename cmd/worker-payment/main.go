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
	useCase := worker.NewPaymentUseCase(gateway, producer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("worker de pagamento: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-payment", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de pagamento: encerrado com erro: %v", err)
	}
	log.Println("worker de pagamento: encerrado")
}
