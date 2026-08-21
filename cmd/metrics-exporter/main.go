package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"workers-kafka/internal/infrastructure/metrics"
	infrapostgres "workers-kafka/internal/infrastructure/persistence/postgres"
)

// metrics-exporter expõe métricas derivadas do banco de escrita: sagas por estado
// (fila/pipeline), COMPLETED, FAILED. Roda no compose (porta 9107) e alimenta o Grafana.
const refreshInterval = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := infrapostgres.Connect(ctx, infrapostgres.DatabaseURLFromEnv())
	if err != nil {
		log.Fatalf("metrics-exporter: %v", err)
	}
	defer pool.Close()

	metrics.Serve(":9107")
	log.Println("metrics-exporter: aguardando métricas do postgres")

	for {
		select {
		case <-ctx.Done():
			log.Println("metrics-exporter: encerrado")
			return
		case <-time.After(refreshInterval):
			refresh(ctx, pool)
		}
	}
}

func refresh(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `SELECT current_status, count(*) FROM sagas GROUP BY current_status`)
	if err != nil {
		log.Printf("metrics-exporter: erro ao consultar sagas: %v", err)
		return
	}
	defer rows.Close()

	totalCompleted, totalFailed := 0, 0
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			log.Printf("metrics-exporter: erro ao ler linha: %v", err)
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
	log.Printf("metrics-exporter: sagas pendentes por status atualizadas; completed=%d failed=%d", totalCompleted, totalFailed)
}
