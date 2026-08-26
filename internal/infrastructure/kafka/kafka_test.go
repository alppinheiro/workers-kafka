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
		{domain.EventPaymentCommand, TopicPaymentCommand},
		{domain.EventPaymentCompensate, TopicPaymentCommand},
		{domain.EventPaymentResult, TopicPaymentResult},
		{domain.EventPaymentCompensateResult, TopicPaymentResult},
		{domain.EventInventoryCommand, TopicInventoryCommand},
		{domain.EventInventoryResult, TopicInventoryResult},
		{domain.EventNotificationCommand, TopicNotificationCommand},
		{domain.EventNotificationResult, TopicNotificationResult},
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

func TestDLQTopicFor(t *testing.T) {
	cases := map[string]string{
		TopicOrderCreated:       TopicOrderCreated + ".dlq",
		TopicPaymentResult:      TopicPaymentResult + ".dlq",
		TopicInventoryResult:    TopicInventoryResult + ".dlq",
		TopicNotificationResult: TopicNotificationResult + ".dlq",
		TopicOrderStatus:        TopicOrderStatus + ".dlq",
	}
	for topic, want := range cases {
		if got := DLQTopicFor(topic); got != want {
			t.Errorf("DLQTopicFor(%q) = %q, esperado %q", topic, got, want)
		}
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

	retryable := []error{
		kafkago.GroupCoordinatorNotAvailable,
		kafkago.NotCoordinatorForGroup,
		kafkago.GroupLoadInProgress,
		kafkago.UnknownTopicOrPartition,
		kafkago.LeaderNotAvailable,
		kafkago.NotLeaderForPartition,
		kafkago.BrokerNotAvailable,
		kafkago.RebalanceInProgress,
		kafkago.UnknownMemberId,
	}
	for _, err := range retryable {
		if !shouldRetryFetch(err) {
			t.Errorf("%v deveria exigir retry", err)
		}
	}
}

func TestShouldRetryCommit(t *testing.T) {
	if !shouldRetryCommit(kafkago.UnknownTopicOrPartition) {
		t.Error("UnknownTopicOrPartition deveria ser retry de commit (não-fatal)")
	}
	if shouldRetryCommit(errors.New("erro comum")) {
		t.Error("erro comum não deveria ser classificado como retry de commit")
	}
}

func TestCommitBatcher_ByCount(t *testing.T) {
	now := time.Now()
	b := newCommitBatcher(50, time.Hour)
	for i := 0; i < 49; i++ {
		b.add()
		if b.shouldFlush(now) {
			t.Fatalf("não deveria flush com %d pendentes (< 50)", i+1)
		}
	}
	b.add() // 50ª mensagem
	if !b.shouldFlush(now) {
		t.Fatal("deveria flush ao atingir o batch de 50")
	}
	b.reset(now)
	if b.shouldFlush(now.Add(time.Millisecond)) {
		t.Fatal("após reset, não deveria flush imediato")
	}
}

func TestCommitBatcher_ByInterval(t *testing.T) {
	b := newCommitBatcher(1_000_000, 200*time.Millisecond)
	now := time.Now()
	b.add()
	if b.shouldFlush(now.Add(100 * time.Millisecond)) {
		t.Fatal("ainda dentro do intervalo, não deveria flush")
	}
	if !b.shouldFlush(now.Add(250 * time.Millisecond)) {
		t.Fatal("passou o intervalo, deveria flush")
	}
}

func TestCommitBatchFromEnv_Defaults(t *testing.T) {
	t.Setenv("KAFKA_COMMIT_BATCH", "")
	if got := CommitBatchFromEnv(); got != 50 {
		t.Errorf("esperado default 50, obtido %d", got)
	}
	t.Setenv("KAFKA_COMMIT_BATCH", "abc")
	if got := CommitBatchFromEnv(); got != 50 {
		t.Errorf("esperado fallback 50 para valor inválido, obtido %d", got)
	}
	t.Setenv("KAFKA_COMMIT_BATCH", "250")
	if got := CommitBatchFromEnv(); got != 250 {
		t.Errorf("esperado 250, obtido %d", got)
	}
}

func TestCommitIntervalFromEnv_Defaults(t *testing.T) {
	t.Setenv("KAFKA_COMMIT_INTERVAL", "")
	if got := CommitIntervalFromEnv(); got != 200*time.Millisecond {
		t.Errorf("esperado default 200ms, obtido %v", got)
	}
	t.Setenv("KAFKA_COMMIT_INTERVAL", "500ms")
	if got := CommitIntervalFromEnv(); got != 500*time.Millisecond {
		t.Errorf("esperado 500ms, obtido %v", got)
	}
}

func TestStallDetected(t *testing.T) {
	if !stallDetected(10, 10, 45*time.Second, 45*time.Second) {
		t.Error("sem avanço de fetch e tempo esgotado deveria detectar stall")
	}
	if stallDetected(11, 10, 60*time.Second, 45*time.Second) {
		t.Error("fetch avançou (11 > 10): não é stall")
	}
	if stallDetected(10, 10, 10*time.Second, 45*time.Second) {
		t.Error("tempo insuficiente: não é stall")
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
