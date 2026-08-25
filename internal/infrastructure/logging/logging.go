package logging

import (
	"log/slog"
	"os"
)

// Setup configura o logger global do serviço com saída JSON estruturada (log/slog),
// incluindo o nome do serviço como atributo em todos os registros. Chamar no início
// do main de cada componente. A saída JSON é ingerida diretamente por CloudWatch/Loki
// sem parser customizado (substitui o log.Printf quase-structured).
func Setup(service string) {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(handler).With("service", service))
}
