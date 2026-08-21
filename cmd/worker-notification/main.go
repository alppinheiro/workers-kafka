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
	"workers-kafka/internal/infrastructure/metrics"
	"workers-kafka/internal/infrastructure/outbox"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/interfaces"
)

// main sobe o worker de notificação, consumindo comandos do tópico orders.notification e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "worker-notification",
		ServiceName: "worker-notification",
		Workers:     infrakafka.WorkersFromEnv(),
		Topic:       infrakafka.TopicOrderNotification,
		DLQWriter:   dlq,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewNotificationSimulator(0.95)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("worker-notification")
	if err != nil {
		log.Fatalf("worker de notificação: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	metrics.Serve(":9104")

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("worker de notificação: %v", err)
	}
	defer pool.Close()

	eventLog := infrapostgres.NewEventLogRepository(pool)
	publisher := outbox.NewPublisher(infrapostgres.NewOutboxRepository(pool))
	useCase := worker.NewNotificationUseCase(gateway, publisher, eventLog)

	log.Println("worker de notificação: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-notification", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de notificação: encerrado com erro: %v", err)
	}
	log.Println("worker de notificação: encerrado")
}
