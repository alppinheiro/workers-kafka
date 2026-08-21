package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/orchestrator"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
	"workers-kafka/internal/interfaces"
)

// main sobe o orquestrador da saga: um único consumer acompanha os três tópicos de
// resultado (pagamento, estoque e notificação) sem depender de goroutines adicionais.
// O estado da saga é persistido em PostgreSQL e as transições são registradas no journal.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: "orchestrator",
		Topics: []string{
			infrakafka.TopicOrderCreated,
			infrakafka.TopicOrderPayment,
			infrakafka.TopicOrderInventory,
			infrakafka.TopicOrderNotification,
		},
	})
	defer func() { _ = consumer.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("orquestrador: %v", err)
	}
	defer pool.Close()

	sagaRepo := infrapostgres.NewSagaRepository(pool)
	eventLog := infrapostgres.NewEventLogRepository(pool)

	orch := orchestrator.New(producer, sagaRepo, eventLog, 3)

	log.Println("orquestrador: aguardando eventos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("orchestrator", orch.HandleEvent)); err != nil && ctx.Err() == nil {
		log.Fatalf("orquestrador: encerrado com erro: %v", err)
	}
	log.Println("orquestrador: encerrado")
}
