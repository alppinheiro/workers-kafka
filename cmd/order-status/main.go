package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"workers-kafka/internal/domain"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/interfaces"
)

// main sobe um consumer de auditoria para os eventos terminais da saga em order status.
func main() {
	logging.Setup("order-status")
	brokers := infrakafka.BrokersFromEnv()

	consumer := infrakafka.NewConsumer(infrakafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: "order-status",
		Topic:   infrakafka.TopicOrderStatus,
		Workers: infrakafka.WorkersFromEnv(),
	})
	defer func() { _ = consumer.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("consumer de status: aguardando eventos terminais")
	if err := consumer.Consume(ctx, interfaces.WithLogging("order-status", handleTerminalEvent)); err != nil && ctx.Err() == nil {
		slog.Error("consumer de status encerrou com erro", "error", err)
		os.Exit(1)
	}
	slog.Info("consumer de status: encerrado")
}

// handleTerminalEvent registra o encerramento final da saga para auditoria simples em log.
func handleTerminalEvent(_ context.Context, event domain.Event) error {
	switch event.EventType {
	case domain.EventOrderCompleted, domain.EventOrderFailed:
		slog.Info("evento terminal da saga",
			"order_id", event.OrderID, "tipo", event.EventType, "status_atual", event.StatusAtual,
			"status_anterior", event.StatusAnterior, "transaction_id", event.TransactionID, "metadata", event.Metadata)
	}

	return nil
}
