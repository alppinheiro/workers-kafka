package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// brokerDialTimeout é o tempo máximo por broker em uma verificação de saúde.
const brokerDialTimeout = 1500 * time.Millisecond

// PingBroker valida a conectividade com um broker abrindo (e fechando) uma conexão
// com handshake do protocolo Kafka. Retorna erro se o broker não responder a tempo.
func PingBroker(ctx context.Context, broker string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("broker %s inacessível: %w", broker, err)
	}
	_ = conn.Close()
	return nil
}

// PingBrokers considera o Kafka saudável se QUALQUER broker responder (os clientes
// kafka-go já fazem failover entre brokers). Cada broker tem um timeout individual
// de dial para não estourar o orçamento total da healthcheck.
func PingBrokers(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("nenhum broker configurado")
	}
	var errs []string
	for _, b := range brokers {
		dialCtx, cancel := context.WithTimeout(ctx, brokerDialTimeout)
		err := PingBroker(dialCtx, b)
		cancel()
		if err == nil {
			return nil
		}
		errs = append(errs, err.Error())
	}
	return fmt.Errorf("kafka inacessível (%d brokers testados): %s", len(brokers), strings.Join(errs, " | "))
}
