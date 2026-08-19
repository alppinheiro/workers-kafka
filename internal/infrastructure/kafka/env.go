package kafka

import (
	"os"
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
