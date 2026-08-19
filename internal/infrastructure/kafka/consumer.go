package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

const consumerRetryDelay = 2 * time.Second

// ConsumerConfig descreve os tópicos que um Consumer deve acompanhar dentro de um consumer group.
type ConsumerConfig struct {
	Brokers []string
	GroupID string
	// Topic é usado quando o consumer acompanha um único tópico.
	Topic string
	// Topics permite acompanhar múltiplos tópicos com um único reader, evitando goroutines adicionais.
	Topics []string
}

// Consumer lê eventos de Kafka e os repassa, já desserializados, para um application.EventHandler.
type Consumer struct {
	reader *kafkago.Reader
}

// NewConsumer cria um consumer a partir da configuração informada.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     cfg.Brokers,
			GroupID:     cfg.GroupID,
			Topic:       cfg.Topic,
			GroupTopics: cfg.Topics,
		}),
	}
}

// Consume lê mensagens continuamente até que o contexto seja encerrado, desserializando e processando cada evento.
func (c *Consumer) Consume(ctx context.Context, handler application.EventHandler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if shouldRetryFetch(err) {
				log.Printf("component=consumer phase=retry-fetch delay=%s error=%v", consumerRetryDelay, err)
				if waitErr := waitForRetry(ctx, consumerRetryDelay); waitErr != nil {
					return waitErr
				}
				continue
			}
			return fmt.Errorf("erro ao ler mensagem: %w", err)
		}

		var event domain.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("erro ao desserializar evento: %w", err)
		}

		if err := handler(ctx, event); err != nil {
			return fmt.Errorf("erro ao processar evento %s: %w", event.EventID, err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("erro ao confirmar mensagem: %w", err)
		}
	}
}

// Close libera os recursos do reader subjacente.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

func shouldRetryFetch(err error) bool {
	return err == kafkago.GroupCoordinatorNotAvailable ||
		err == kafkago.NotCoordinatorForGroup ||
		err == kafkago.GroupLoadInProgress
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
