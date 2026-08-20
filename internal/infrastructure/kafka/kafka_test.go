package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"workers-kafka/internal/domain"
)

func TestTopicForEventType(t *testing.T) {
	cases := []struct {
		eventType domain.EventType
		expected  string
	}{
		{domain.EventOrderCreated, TopicOrderCreated},
		{domain.EventOrderCompleted, TopicOrderStatus},
		{domain.EventOrderFailed, TopicOrderStatus},
		{domain.EventPaymentCommand, TopicOrderPayment},
		{domain.EventPaymentCompensate, TopicOrderPayment},
		{domain.EventPaymentResult, TopicOrderPayment},
		{domain.EventPaymentCompensateResult, TopicOrderPayment},
		{domain.EventInventoryCommand, TopicOrderInventory},
		{domain.EventInventoryResult, TopicOrderInventory},
		{domain.EventNotificationCommand, TopicOrderNotification},
		{domain.EventNotificationResult, TopicOrderNotification},
	}

	for _, tc := range cases {
		t.Run(string(tc.eventType), func(t *testing.T) {
			if got := topicForEventType[tc.eventType]; got != tc.expected {
				t.Errorf("tópico esperado %q, obtido %q", tc.expected, got)
			}
		})
	}
}

func TestBrokersFromEnv_Default(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	brokers := BrokersFromEnv()
	if len(brokers) != 1 || brokers[0] != "localhost:9092" {
		t.Errorf("esperado default [localhost:9092], obtido %v", brokers)
	}
}

func TestBrokersFromEnv_Multiple(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092,broker3:9092")
	brokers := BrokersFromEnv()
	if len(brokers) != 3 {
		t.Fatalf("esperado 3 brokers, obtido %v", brokers)
	}
	if brokers[0] != "broker1:9092" || brokers[2] != "broker3:9092" {
		t.Errorf("brokers incorretos: %v", brokers)
	}
}

func TestFormatMetadata_Empty(t *testing.T) {
	if got := formatMetadata(nil); got != "-" {
		t.Errorf("esperado '-', obtido %q", got)
	}
	if got := formatMetadata(map[string]string{}); got != "-" {
		t.Errorf("esperado '-' para mapa vazio, obtido %q", got)
	}
}

func TestFormatMetadata_Sorted(t *testing.T) {
	meta := map[string]string{"z": "1", "a": "2", "m": "3"}
	got := formatMetadata(meta)
	expected := "a=2,m=3,z=1"
	if got != expected {
		t.Errorf("esperado %q, obtido %q", expected, got)
	}
}

func TestProducer_Publish_UnknownEventType(t *testing.T) {
	p := &Producer{writer: nil}
	event := domain.Event{EventType: domain.EventType("TIPO_DESCONHECIDO")}

	err := p.Publish(context.Background(), event)
	if err == nil {
		t.Fatal("esperado erro para tipo de evento sem tópico")
	}
}

func TestShouldRetryFetch(t *testing.T) {
	if shouldRetryFetch(errors.New("erro comum")) {
		t.Error("erro comum não deveria ser classificado como retry")
	}

	if !shouldRetryFetch(kafkago.GroupCoordinatorNotAvailable) {
		t.Error("GroupCoordinatorNotAvailable deveria exigir retry")
	}
	if !shouldRetryFetch(kafkago.NotCoordinatorForGroup) {
		t.Error("NotCoordinatorForGroup deveria exigir retry")
	}
	if !shouldRetryFetch(kafkago.GroupLoadInProgress) {
		t.Error("GroupLoadInProgress deveria exigir retry")
	}
}

func TestWaitForRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForRetry(ctx, 50*time.Millisecond)
	if err == nil {
		t.Fatal("esperado erro de contexto cancelado")
	}
}

func TestWaitForRetry_TimerExpires(t *testing.T) {
	startedAt := time.Now()
	err := waitForRetry(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatalf("esperado nil após expirar o timer, obtido %v", err)
	}
	if time.Since(startedAt) < 10*time.Millisecond {
		t.Error("waitForRetry retornou antes do delay previsto")
	}
}
