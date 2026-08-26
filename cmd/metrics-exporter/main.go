package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"workers-kafka/internal/infrastructure/health"
	infrakafka "workers-kafka/internal/infrastructure/kafka"
	"workers-kafka/internal/infrastructure/logging"
	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
)

// metrics-exporter expõe métricas derivadas do banco de escrita e do Kafka:
// sagas por estado, COMPLETED/FAILED, idade do evento mais antigo na outbox e
// lag por consumer group. Roda no compose (porta 9107) e alimenta o Grafana.
const refreshInterval = 10 * time.Second

var (
	consumerGroups = []string{"orchestrator", "worker-payment", "worker-inventory", "worker-notification", "projector", "order-status"}
	flowTopics     = infrakafka.FlowTopics()
	flowPartitions = []int{0, 1, 2, 3}
)

func main() {
	logging.Setup("metrics-exporter")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		slog.Error("falha ao conectar no postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	brokers := infrakafka.BrokersFromEnv()
	addr := kafkago.TCP(brokers...)
	client := &kafkago.Client{Addr: addr, Timeout: 10 * time.Second}

	metrics.ServeWithChecks(":9107", health.Postgres(pool), health.Kafka(brokers))
	slog.Info("metrics-exporter: aguardando métricas do postgres/kafka")

	for {
		select {
		case <-ctx.Done():
			slog.Info("metrics-exporter: encerrado")
			return
		case <-time.After(refreshInterval):
			refreshSagas(ctx, pool)
			refreshOutboxAge(ctx, pool)
			refreshConsumerLag(ctx, client, addr)
		}
	}
}

// refreshSagas atualiza os gauges de sagas por status. ResetOrdersPending é chamado
// ANTES do Set para zerar labels de status que não existem mais (anti-gauge-stale).
func refreshSagas(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `SELECT current_status, count(*) FROM sagas GROUP BY current_status`)
	if err != nil {
		slog.Error("erro ao consultar sagas", "error", err)
		return
	}
	defer rows.Close()

	metrics.ResetOrdersPending()
	totalCompleted, totalFailed := 0, 0
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			slog.Error("erro ao ler linha de sagas", "error", err)
			return
		}
		switch status {
		case "COMPLETED":
			totalCompleted = n
		case "FAILED":
			totalFailed = n
		default:
			metrics.SetOrdersPending(status, n)
		}
	}

	metrics.SetOrdersCompleted(totalCompleted)
	metrics.SetOrdersFailed(totalFailed)
}

// refreshOutboxAge atualiza a idade (em segundos) do evento mais antigo ainda não
// publicado na outbox — 0 quando a outbox está vazia.
func refreshOutboxAge(ctx context.Context, pool *pgxpool.Pool) {
	var seconds float64
	err := pool.QueryRow(ctx,
		`SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0) FROM outbox WHERE published_at IS NULL`,
	).Scan(&seconds)
	if err != nil {
		slog.Error("erro ao consultar idade da outbox", "error", err)
		return
	}
	metrics.SetOutboxMaxAge(seconds)
}

// refreshConsumerLag expõe o lag de cada consumer group por tópico (mensagens
// publicadas mas ainda não commitadas pelo grupo), via API de admin do Kafka.
func refreshConsumerLag(ctx context.Context, client *kafkago.Client, addr net.Addr) {
	// Fim de cada tópico/partição (uma request para todos os tópicos).
	offsetsReq := &kafkago.ListOffsetsRequest{
		Addr:           addr,
		Topics:         map[string][]kafkago.OffsetRequest{},
		IsolationLevel: kafkago.ReadUncommitted,
	}
	for _, topic := range flowTopics {
		parts := make([]kafkago.OffsetRequest, 0, len(flowPartitions))
		for _, p := range flowPartitions {
			parts = append(parts, kafkago.LastOffsetOf(p))
		}
		offsetsReq.Topics[topic] = parts
	}
	endResp, err := client.ListOffsets(ctx, offsetsReq)
	if err != nil {
		slog.Error("erro ao ler fim dos tópicos", "error", err)
		return
	}

	for _, group := range consumerGroups {
		topicsReq := map[string][]int{}
		for _, topic := range flowTopics {
			topicsReq[topic] = flowPartitions
		}
		fetchResp, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{Addr: addr, GroupID: group, Topics: topicsReq})
		if err != nil {
			slog.Error("erro ao ler offsets do grupo", "group", group, "error", err)
			continue
		}

		for _, topic := range flowTopics {
			var lag int64
			for _, ep := range endResp.Topics[topic] {
				for _, cp := range fetchResp.Topics[topic] {
					if cp.Partition == ep.Partition {
						// CommittedOffset < 0 indica partição sem offset commitado (grupo novo).
						if cp.CommittedOffset >= 0 && ep.LastOffset > cp.CommittedOffset {
							lag += ep.LastOffset - cp.CommittedOffset
						}
					}
				}
			}
			metrics.SetConsumerLag(group, topic, lag)
		}
	}
}
