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

// main sobe o worker de notificação, consumindo comandos do tópico orders.notification e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: "worker-notification",
		Topic:   infrakafka.TopicOrderNotification,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewNotificationSimulator(0.95)
	useCase := worker.NewNotificationUseCase(gateway, producer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("worker de notificação: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-notification", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de notificação: encerrado com erro: %v", err)
	}
	log.Println("worker de notificação: encerrado")
}
