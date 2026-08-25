package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"workers-kafka/internal/domain"
	"workers-kafka/internal/infrastructure/metrics"
)

// Producer publica eventos de domínio em Kafka, serializados em JSON, usando order_id como chave de particionamento.
type Producer struct {
	writer      *kafkago.Writer
	serviceName string
}

// NewProducer cria um producer conectado aos brokers informados, identificando o serviço
// nas métricas de publicação.
func NewProducer(brokers []string, serviceName string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.Hash{},
			RequiredAcks: AcksFromEnv(),
		},
		serviceName: serviceName,
	}
}

// Publish resolve o tópico a partir do EventType e envia o evento serializado em JSON.
func (p *Producer) Publish(ctx context.Context, event domain.Event) error {
	topic, ok := topicForEventType[event.EventType]
	if !ok {
		return fmt.Errorf("nenhum tópico configurado para o tipo de evento %s", event.EventType)
	}

	startedAt := time.Now()
	slog.InfoContext(ctx, "publicando evento",
		"component", "producer", "phase", "publishing", "topic", topic,
		"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
		"type", event.EventType, "status_previous", event.StatusAnterior, "status_current", event.StatusAtual,
		"schema_version", event.SchemaVersion, "metadata", formatMetadata(event.Metadata),
	)

	payload, err := json.Marshal(event)
	if err != nil {
		slog.ErrorContext(ctx, "erro ao serializar evento",
			"component", "producer", "phase", "failed", "topic", topic,
			"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
			"type", event.EventType, "duration", time.Since(startedAt), "error", err,
		)
		return fmt.Errorf("erro ao serializar evento %s: %w", event.EventID, err)
	}

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     []byte(event.OrderID),
		Value:   payload,
		Headers: injectTraceHeaders(ctx),
	}); err != nil {
		slog.ErrorContext(ctx, "erro ao publicar evento",
			"component", "producer", "phase", "failed", "topic", topic,
			"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
			"type", event.EventType, "duration", time.Since(startedAt), "error", err,
		)
		return err
	}
	metrics.RecordPublished(p.serviceName, topic)

	slog.InfoContext(ctx, "evento publicado",
		"component", "producer", "phase", "published", "topic", topic,
		"event_id", event.EventID, "order_id", event.OrderID, "saga_id", event.SagaID,
		"type", event.EventType, "status_previous", event.StatusAnterior, "status_current", event.StatusAtual,
		"payload_bytes", len(payload), "duration", time.Since(startedAt),
	)

	// Loga também o transaction_id quando presente para melhor observabilidade.
	if event.TransactionID != "" {
		slog.InfoContext(ctx, "evento com transação",
			"component", "producer", "transaction_id", event.TransactionID,
			"event_id", event.EventID, "order_id", event.OrderID, "type", event.EventType,
		)
	}

	return nil
}

// PublishRaw publica um payload já serializado em um tópico específico (usado pelo
// outbox-relay, que não precisa desserializar o evento).
func (p *Producer) PublishRaw(ctx context.Context, topic string, key string, payload []byte) error {
	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   payload,
		Headers: injectTraceHeaders(ctx),
	}); err != nil {
		return err
	}
	return nil
}

// PublishBatch publica múltiplas mensagens em uma única chamada ao Kafka, evitando o
// custo de um round-trip por mensagem (usado pelo outbox-relay para alta vazão).
func (p *Producer) PublishBatch(ctx context.Context, msgs []kafkago.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return err
	}
	for _, m := range msgs {
		metrics.RecordPublished(p.serviceName, m.Topic)
	}
	return nil
}

// TraceHeadersFrom serializa o contexto de trace corrente (W3C traceparent) nos headers
// do Kafka, para propagação distribuída (usado pelo outbox-relay ao publicar em lote).
func TraceHeadersFrom(ctx context.Context) []kafkago.Header {
	return injectTraceHeaders(ctx)
}

// injectTraceHeaders serializa o contexto de trace corrente (W3C traceparent) nos
// headers da mensagem para propagação distribuída entre os componentes.
func injectTraceHeaders(ctx context.Context) []kafkago.Header {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	headers := make([]kafkago.Header, 0, len(carrier))
	for k, v := range carrier {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return headers
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
