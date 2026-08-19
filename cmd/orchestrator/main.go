package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/application/orchestrator"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/interfaces"
)

// main sobe o orquestrador da saga: um único consumer acompanha os três tópicos de resultado
// (pagamento, estoque e notificação) sem depender de goroutines adicionais.
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

	orch := orchestrator.New(producer, 3)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("orquestrador: aguardando eventos")
	if err := consumer.Consume(ctx, interfaces.WithLogging("orchestrator", orch.HandleEvent)); err != nil && ctx.Err() == nil {
		log.Fatalf("orquestrador: encerrado com erro: %v", err)
	}
	log.Println("orquestrador: encerrado")
}
