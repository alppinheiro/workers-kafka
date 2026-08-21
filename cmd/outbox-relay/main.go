package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	infrakafka "workers-kafka/internal/infrastructure/kafka"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
)

const (
	pollInterval = time.Second
	batchSize    = 100
)

// main roda o relé da outbox: lê eventos não publicados da tabela outbox, publica no
// Kafka e marca como publicado. É o componente que garante a entrega dos eventos
// decididos pelos orquestrador/workers, mesmo que o processo que os gerou tenha morrido.
func main() {
	brokers := infrakafka.BrokersFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("outbox-relay: %v", err)
	}
	defer pool.Close()

	outbox := infrapostgres.NewOutboxRepository(pool)
	producer := infrakafka.NewProducer(brokers)
	defer func() { _ = producer.Close() }()

	log.Println("outbox-relay: aguardando eventos na outbox")
	for {
		select {
		case <-ctx.Done():
			log.Println("outbox-relay: encerrado")
			return
		case <-time.After(pollInterval):
			if err := relayOnce(ctx, outbox, producer); err != nil {
				log.Printf("outbox-relay: erro no ciclo: %v", err)
			}
		}
	}
}

func relayOnce(ctx context.Context, outbox *infrapostgres.OutboxRepository, producer *infrakafka.Producer) error {
	entries, err := outbox.FetchPending(ctx, batchSize)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := producer.PublishRaw(ctx, entry.Topic, entry.Key, entry.Payload); err != nil {
			return fmt.Errorf("erro ao publicar evento %s na outbox: %w", entry.EventID, err)
		}
		if err := outbox.MarkPublished(ctx, entry.ID); err != nil {
			return fmt.Errorf("erro ao marcar evento %s como publicado: %w", entry.EventID, err)
		}
		log.Printf("outbox-relay: publicado event_id=%s topic=%s key=%s", entry.EventID, entry.Topic, entry.Key)
	}
	return nil
}
