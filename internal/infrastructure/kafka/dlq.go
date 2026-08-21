package kafka

import kafkago "github.com/segmentio/kafka-go"

// NewDLQWriter cria um writer usado para mover mensagens com erro definitivo para os
// tópicos DLQ correspondentes.
func NewDLQWriter(brokers []string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireOne,
	}
}
