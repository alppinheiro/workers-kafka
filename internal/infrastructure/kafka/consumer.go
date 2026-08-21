package kafka

import (
	"context"
	"encoding/json"
	"errors"
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
	// DLQWriter, quando definido, recebe mensagens com erro definitivo (application.ErrNonRetryable),
	// que são movidas para o tópico DLQ correspondente e commitadas.
	DLQWriter *kafkago.Writer
}

// Consumer lê eventos de Kafka e os repassa, já desserializados, para um application.EventHandler.
type Consumer struct {
	reader *kafkago.Reader
	dlq    *kafkago.Writer
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
		dlq: cfg.DLQWriter,
	}
}

// Consume lê mensagens continuamente até que o contexto seja encerrado, desserializando e
// processando cada evento. Mensagens com erro definitivo são movidas para a DLQ; demais
// erros são retornados (a mensagem não é commitada e será reentregue).
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
			moved, moveErr := c.moveToDLQ(ctx, msg, fmt.Errorf("%w: erro ao desserializar evento: %v", application.ErrNonRetryable, err))
			if moveErr != nil {
				return moveErr
			}
			if moved {
				continue
			}
			return fmt.Errorf("erro ao desserializar evento: %w", err)
		}

		if err := handler(ctx, event); err != nil {
			moved, moveErr := c.moveToDLQ(ctx, msg, fmt.Errorf("erro ao processar evento %s: %w", event.EventID, err))
			if moveErr != nil {
				return moveErr
			}
			if moved {
				continue
			}
			return err
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("erro ao confirmar mensagem: %w", err)
		}
	}
}

// moveToDLQ move uma mensagem com erro definitivo para a DLQ do tópico de origem e a
// commita, retornando true. Se não houver DLQ configurada ou o erro não for definitivo
// (transitório), retorna false e a mensagem permanece pendente para retry.
func (c *Consumer) moveToDLQ(ctx context.Context, msg kafkago.Message, err error) (bool, error) {
	if c.dlq == nil || !errors.Is(err, application.ErrNonRetryable) {
		return false, nil
	}

	dlqTopic := DLQTopicFor(msg.Topic)
	if writeErr := c.dlq.WriteMessages(ctx, kafkago.Message{Topic: dlqTopic, Key: msg.Key, Value: msg.Value}); writeErr != nil {
		return false, fmt.Errorf("erro ao mover mensagem para a DLQ %s: %w", dlqTopic, writeErr)
	}

	log.Printf("component=consumer phase=dlq topic=%s dlq_topic=%s offset=%d error=%v", msg.Topic, dlqTopic, msg.Offset, err)

	if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
		return false, fmt.Errorf("erro ao confirmar mensagem movida para a DLQ: %w", commitErr)
	}
	return true, nil
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
