package main

import (
	"context"
	"log"
	"os"

	"workers-kafka/internal/application/worker"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
)

// main publica o evento inicial de um novo pedido (PENDING), disparando o início da saga.
// Uso: go run ./cmd/create-order <order_id>
func main() {
	if len(os.Args) < 2 {
		log.Fatal("uso: create-order <order_id>")
	}
	orderID := os.Args[1]

	brokers := infrakafka.BrokersFromEnv()

	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	coordinator := worker.NewCoordinator(producer)

	if err := coordinator.CreateOrder(context.Background(), orderID); err != nil {
		log.Fatalf("erro ao criar pedido: %v", err)
	}

	log.Printf("pedido %s criado com status PENDING", orderID)
}
