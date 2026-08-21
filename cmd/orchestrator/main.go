package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/orchestrator"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
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
		log.Fatalf("orquestrador: falha ao inicializar telemetria: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	metrics.Serve(":9101")

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("orquestrador: %v", err)
	}
	defer pool.Close()

	orch := orchestrator.New(uow.New(pool), 3)

	log.Println("orquestrador: aguardando eventos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("orchestrator", orch.HandleEvent)); err != nil && ctx.Err() == nil {
		log.Fatalf("orquestrador: encerrado com erro: %v", err)
	}
	log.Println("orquestrador: encerrado")
}
