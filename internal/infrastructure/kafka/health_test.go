package kafka

import (
	"context"
	"testing"
)

func TestPingBrokers_NoBrokers(t *testing.T) {
	if err := PingBrokers(context.Background(), nil); err == nil {
		t.Fatal("deveria falhar sem brokers configurados")
	}
}

func TestPingBrokers_Unreachable(t *testing.T) {
	// Porta 1 não tem serviço: o dial falha com connection refused (rápido, sem rede).
	if err := PingBrokers(context.Background(), []string{"localhost:1"}); err == nil {
		t.Fatal("deveria falhar com broker inacessível")
	}
}
