package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"workers-kafka/internal/domain"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
)

// load-generator publica N eventos ORDER_CREATED no tópico orders.created em lotes,
// medindo a vazão real de ingestão. Uso (após make up):
//
//	go run ./cmd/load-generator -count 2000 -batch 500
//	go run ./cmd/load-generator -count 120000 -rate 2000   # sustentar 2.000 ev/s por 60s
//
// Use para observar o backlog/lag e o throughput de processamento das sagas.
func main() {
	logging.Setup("load-generator")
	count := flag.Int("count", 10000, "total de pedidos a publicar")
	batch := flag.Int("batch", 500, "tamanho do lote por WriteMessages")
	prefix := flag.String("prefix", "load", "prefixo do order_id")
	rate := flag.Int("rate", 0, "taxa máxima de publicação (eventos/s); 0 = o mais rápido possível")
	flag.Parse()

	brokers := infrakafka.BrokersFromEnv()

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Balancer:     &kafkago.Hash{},
		RequiredAcks: infrakafka.AcksFromEnv(),
	}
	defer func() { _ = writer.Close() }()

	ctx := context.Background()
	start := time.Now()
	published := 0

	for published < *count {
		msgs := make([]kafkago.Message, 0, *batch)
		for i := 0; i < *batch && published < *count; i++ {
			orderID := fmt.Sprintf("%s-%06d", *prefix, published)
			event := domain.Event{
				EventID:       domain.NewEventID(),
				OrderID:       orderID,
				SagaID:        orderID,
				StatusAtual:   domain.StatusPending,
				EventType:     domain.EventOrderCreated,
				SchemaVersion: domain.CurrentSchemaVersion,
				CreatedAt:     time.Now().UTC(),
			}
			payload, err := json.Marshal(event)
			if err != nil {
				slog.Error("erro ao serializar evento", "error", err)
				os.Exit(1)
			}
			msgs = append(msgs, kafkago.Message{
				Topic: infrakafka.TopicOrderCreated,
				Key:   []byte(orderID),
				Value: payload,
			})
			published++
		}

		if err := writer.WriteMessages(ctx, msgs...); err != nil {
			slog.Error("erro ao publicar lote", "error", err)
			os.Exit(1)
		}

		// Rate limiting: com -rate > 0, mantém a taxa MÉDIA de publicação (eventos/s)
		// aguardando o tempo necessário para o total publicado até aqui caber na janela.
		if *rate > 0 {
			targetElapsed := time.Duration(published) * time.Second / time.Duration(*rate)
			if wait := targetElapsed - time.Since(start); wait > 0 {
				time.Sleep(wait)
			}
		}
	}

	elapsed := time.Since(start)
	slog.Info("load-generator finalizado", "pedidos_publicados", published, "duracao", elapsed, "eventos_por_segundo", float64(published)/elapsed.Seconds())
}
