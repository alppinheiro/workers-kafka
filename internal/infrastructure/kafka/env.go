package kafka

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// BrokersFromEnv lê os endereços dos brokers Kafka da variável KAFKA_BROKERS (separados por vírgula),
// usando localhost:9092 como padrão para desenvolvimento local.
func BrokersFromEnv() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return []string{"localhost:9092"}
	}
	return strings.Split(raw, ",")
}

// WorkersFromEnv lê o número de goroutines de consumo por instância (SAGA_WORKERS).
// Default 1 = comportamento sequencial original. Valores < 1 são ignorados.
func WorkersFromEnv() int {
	raw := os.Getenv("SAGA_WORKERS")
	if raw == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// CommitBatchFromEnv lê quantas mensagens acumular antes de commitar os offsets em lote
// (KAFKA_COMMIT_BATCH). Reduz os round-trips ao broker (default 50).
func CommitBatchFromEnv() int {
	raw := os.Getenv("KAFKA_COMMIT_BATCH")
	if raw == "" {
		return 50
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 50
	}
	return n
}

// CommitIntervalFromEnv lê o intervalo máximo entre commits em lote
// (KAFKA_COMMIT_INTERVAL, default 200ms).
func CommitIntervalFromEnv() time.Duration {
	raw := os.Getenv("KAFKA_COMMIT_INTERVAL")
	if raw == "" {
		return 200 * time.Millisecond
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 10*time.Millisecond {
		return 200 * time.Millisecond
	}
	return d
}
