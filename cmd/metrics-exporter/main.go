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

	// allSagaStatuses são todos os status do ciclo de vida da saga (domain.OrderStatus),
	// usados para expor a visão completa em saga_orders_by_status — inclusive os terminais
	// (COMPLETED/FAILED) e os transientes que podem estar com 0 no momento.
	allSagaStatuses = []string{
		"PENDING", "PAYMENT_PENDING", "PAYMENT_APPROVED", "PAYMENT_REFUND_PENDING",
		"INVENTORY_RESERVED", "NOTIFIED", "COMPLETED", "PAYMENT_REFUNDED", "RETRYING", "FAILED",
	}

	// flowDLQTopics são os tópicos de DLQ do pipeline (1 partição cada no compose/kind).
	flowDLQTopics = func() []string {
		dlqs := make([]string, 0, len(flowTopics))
		for _, t := range flowTopics {
			dlqs = append(dlqs, infrakafka.DLQTopicFor(t))
		}
		return dlqs
	}()
	dlqPartitions = []int{0}
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
			refreshSagaAges(ctx, pool)
			refreshOutboxAge(ctx, pool)
			refreshConsumerLag(ctx, client, addr)
			refreshDLQDepth(ctx, client, addr)
		}
	}
}

// refreshSagas atualiza os gauges de sagas por status. ResetOrdersPending é chamado
// ANTES do Set para zerar labels de status que não existem mais (anti-gauge-stale).
// A visão completa (saga_orders_by_status) é alimentada com TODOS os status conhecidos,
// inclusive terminais e os que estão com 0, para o dashboard exibir a imagem inteira.
func refreshSagas(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `SELECT current_status, count(*) FROM sagas GROUP BY current_status`)
	if err != nil {
		slog.Error("erro ao consultar sagas", "error", err)
		return
	}
	defer rows.Close()

	counts := make(map[string]int, len(allSagaStatuses))
	for _, s := range allSagaStatuses {
		counts[s] = 0
	}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			slog.Error("erro ao ler linha de sagas", "error", err)
			return
		}
		counts[status] = n
	}

	metrics.ResetOrdersPending()
	metrics.ResetOrdersByStatus()
	for _, s := range allSagaStatuses {
		n := counts[s]
		metrics.SetOrdersByStatus(s, n)
		if s == "COMPLETED" || s == "FAILED" {
			continue
		}
		metrics.SetOrdersPending(s, n)
	}
	metrics.SetOrdersCompleted(counts["COMPLETED"])
	metrics.SetOrdersFailed(counts["FAILED"])
}

// refreshSagaAges expõe a idade (s) da saga mais antiga ainda em cada status
// intermediário — permite alertar "pedido preso" sem depender de logs.
func refreshSagaAges(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT current_status, COALESCE(EXTRACT(EPOCH FROM (now() - MAX(created_at))), 0)
		FROM sagas
		WHERE current_status NOT IN ('COMPLETED', 'FAILED')
		GROUP BY current_status`)
	if err != nil {
		slog.Error("erro ao consultar idade das sagas", "error", err)
		return
	}
	defer rows.Close()

	metrics.ResetSagaMaxAge()
	for rows.Next() {
		var status string
		var seconds float64
		if err := rows.Scan(&status, &seconds); err != nil {
			slog.Error("erro ao ler idade da saga", "error", err)
			return
		}
		metrics.SetSagaMaxAge(status, seconds)
	}
}

// refreshDLQDepth expõe quantas mensagens estão acumuladas em cada tópico de DLQ
// (offset final do tópico; DLQ não é consumida — o depth é o backlog real).
func refreshDLQDepth(ctx context.Context, client *kafkago.Client, addr net.Addr) {
	req := &kafkago.ListOffsetsRequest{
		Addr:           addr,
		Topics:         map[string][]kafkago.OffsetRequest{},
		IsolationLevel: kafkago.ReadUncommitted,
	}
	for _, dlq := range flowDLQTopics {
		parts := make([]kafkago.OffsetRequest, 0, len(dlqPartitions))
		for _, p := range dlqPartitions {
			parts = append(parts, kafkago.LastOffsetOf(p))
		}
		req.Topics[dlq] = parts
	}

	resp, err := client.ListOffsets(ctx, req)
	if err != nil {
		slog.Error("erro ao ler fim das DLQs", "error", err)
		return
	}

	for _, dlq := range flowDLQTopics {
		parts, ok := resp.Topics[dlq]
		if !ok {
			continue
		}
		for _, p := range parts {
			if p.Partition == 0 && p.Error == nil {
				metrics.SetDLQDepth(dlq, p.LastOffset)
			}
		}
	}
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
