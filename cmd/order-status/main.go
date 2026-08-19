package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/domain"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/interfaces"
)

// main sobe um consumer de auditoria para os eventos terminais da saga em order status.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: "order-status",
		Topic:   infrakafka.TopicOrderStatus,
	})
	defer func() { _ = consumer.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("consumer de status: aguardando eventos terminais")
	if err := consumer.Consume(ctx, interfaces.WithLogging("order-status", handleTerminalEvent)); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer de status: encerrado com erro: %v", err)
	}
	log.Println("consumer de status: encerrado")
}

// handleTerminalEvent registra o encerramento final da saga para auditoria simples em log.
func handleTerminalEvent(_ context.Context, event domain.Event) error {
	switch event.EventType {
	case domain.EventOrderCompleted, domain.EventOrderFailed:
		log.Printf("evento terminal: order_id=%s tipo=%s status_atual=%s status_anterior=%s", event.OrderID, event.EventType, event.StatusAtual, event.StatusAnterior)
	}

	return nil
}
