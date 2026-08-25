package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/worker"
	"workers-kafka/internal/infrastructure/external"
	"workers-kafka/internal/infrastructure/health"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/infrastructure/telemetry"
	"workers-kafka/internal/infrastructure/uow"
	"workers-kafka/internal/interfaces"
)

// main sobe o worker de pagamento, consumindo comandos do tópico orders.payment e publicando o resultado.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	dlq := infrakafka.NewDLQWriter(brokers)
	defer func() { _ = dlq.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "worker-payment",
		ServiceName: "worker-payment",
		Workers:     infrakafka.WorkersFromEnv(),
		Topic:       infrakafka.TopicOrderPayment,
		DLQWriter:   dlq,
	})
	defer func() { _ = consumer.Close() }()

	gateway := external.NewPaymentSimulator(0.85)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init("worker-payment")
	if err != nil {
		log.Fatalf("worker de pagamento: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("worker de pagamento: %v", err)
	}
	defer pool.Close()

	metrics.ServeWithChecks(":9102", health.Postgres(pool), health.Kafka(brokers))

	useCase := worker.NewPaymentUseCase(external.PaymentGatewayFromEnv(gateway), uow.New(pool))

	log.Println("worker de pagamento: aguardando comandos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("worker-payment", useCase.Handle)); err != nil && ctx.Err() == nil {
		log.Fatalf("worker de pagamento: encerrado com erro: %v", err)
	}
	log.Println("worker de pagamento: encerrado")
}
