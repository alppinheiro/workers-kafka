package kafka

import (
	"os"
	"strconv"
	"strings"
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
