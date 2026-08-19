package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"workers-kafka/internal/domain"
)

// Producer publica eventos de domínio em Kafka, serializados em JSON, usando order_id como chave de particionamento.
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer cria um producer conectado aos brokers informados.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireOne,
		},
	}
}

// Publish resolve o tópico a partir do EventType e envia o evento serializado em JSON.
func (p *Producer) Publish(ctx context.Context, event domain.Event) error {
	topic, ok := topicForEventType[event.EventType]
	if !ok {
		return fmt.Errorf("nenhum tópico configurado para o tipo de evento %s", event.EventType)
	}

	startedAt := time.Now()
	log.Printf("component=producer phase=publishing topic=%s event_id=%s order_id=%s saga_id=%s type=%s status_previous=%s status_current=%s schema_version=%d metadata=%s",
		topic,
		event.EventID,
		event.OrderID,
		event.SagaID,
		event.EventType,
		event.StatusAnterior,
		event.StatusAtual,
		event.SchemaVersion,
		formatMetadata(event.Metadata),
	)

	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("component=producer phase=failed topic=%s event_id=%s order_id=%s saga_id=%s type=%s duration=%s error=%v",
			topic,
			event.EventID,
			event.OrderID,
			event.SagaID,
			event.EventType,
			time.Since(startedAt),
			err,
		)
		return fmt.Errorf("erro ao serializar evento %s: %w", event.EventID, err)
	}

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(event.OrderID),
		Value: payload,
	}); err != nil {
		log.Printf("component=producer phase=failed topic=%s event_id=%s order_id=%s saga_id=%s type=%s duration=%s error=%v",
			topic,
			event.EventID,
			event.OrderID,
			event.SagaID,
			event.EventType,
			time.Since(startedAt),
			err,
		)
		return err
	}

	log.Printf("component=producer phase=published topic=%s event_id=%s order_id=%s saga_id=%s type=%s status_previous=%s status_current=%s payload_bytes=%d duration=%s",
		topic,
		event.EventID,
		event.OrderID,
		event.SagaID,
		event.EventType,
		event.StatusAnterior,
		event.StatusAtual,
		len(payload),
		time.Since(startedAt),
	)

// Also log transaction id when present for better observability
if event.TransactionID != "" {
    log.Printf("component=producer transaction_id=%s event_id=%s order_id=%s type=%s", event.TransactionID, event.EventID, event.OrderID, event.EventType)
}

	return nil
}

// Close libera os recursos do writer subjacente.
func (p *Producer) Close() error {
	return p.writer.Close()
}

func formatMetadata(metadata map[string]string) string {
	if len(metadata) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, metadata[key]))
	}

	return strings.Join(parts, ",")
}
