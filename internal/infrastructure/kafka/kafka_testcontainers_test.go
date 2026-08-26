//go:build integration

package kafka_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/kafka"

	"workers-kafka/internal/domain"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
)

// TestKafkaProducerConsumerRoundTrip valida o barramento real: publica um evento via
// Producer e o recebe via Consumer (Kafka em container — Testcontainers).
func TestKafkaProducerConsumerRoundTrip(t *testing.T) {
	ctx := context.Background()

	container, err := kafka.Run(ctx, "confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("test-cluster-id-001"))
	if err != nil {
		t.Fatalf("falha ao subir kafka (testcontainers): %v", err)
	}
	defer func() { _ = container.Terminate(ctx) }()

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("falha ao obter brokers: %v", err)
	}
	t.Logf("brokers retornados: %v", brokers)
	createTopicViaExec(t, container, infrakafka.TopicPaymentResult)

	producer := infrakafka.NewProducer(brokers, "test-producer")
	defer func() { _ = producer.Close() }()

	received := make(chan domain.Event, 1)
	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers:     brokers,
		GroupID:     "test-roundtrip",
		ServiceName: "test-consumer",
		Topic:       infrakafka.TopicPaymentResult,
	})
	defer func() { _ = consumer.Close() }()

	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Consume(consumerCtx, func(_ context.Context, e domain.Event) error {
			received <- e
			return nil
		})
	}()

	event := domain.Event{
		EventID:       "evt-roundtrip-1",
		OrderID:       "order-roundtrip",
		SagaID:        "order-roundtrip",
		StatusAtual:   domain.StatusPaymentPending,
		EventType:     domain.EventPaymentCommand,
		SchemaVersion: domain.CurrentSchemaVersion,
		CreatedAt:     time.Now().UTC(),
	}
	if err := producer.Publish(ctx, event); err != nil {
		t.Fatalf("falha ao publicar evento: %v", err)
	}

	select {
	case got := <-received:
		if got.EventID != event.EventID || got.OrderID != event.OrderID {
			t.Errorf("evento recebido incorreto: %+v", got)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout aguardando o evento no Kafka real")
	}
}

// createTopicViaExec cria o tópico dentro do container Kafka (bootstrap local),
// contornando a limitação do listener BROKER (hostname interno) para o cliente externo.
func createTopicViaExec(t *testing.T, container *kafka.KafkaContainer, topic string) {
	t.Helper()
	ctx := context.Background()
	_, reader, err := container.Exec(ctx, []string{
		"kafka-topics",
		"--bootstrap-server", "localhost:9092",
		"--create", "--if-not-exists",
		"--topic", topic,
		"--partitions", "1",
		"--replication-factor", "1",
	})
	if err != nil {
		t.Fatalf("falha ao executar kafka-topics: %v", err)
	}
	out, _ := io.ReadAll(reader)
	t.Logf("kafka-topics: %s", string(out))
}
