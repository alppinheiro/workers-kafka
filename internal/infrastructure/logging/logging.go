package logging

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// orderIDKey carrega no context o order_id/correlation_id do fluxo atual (pedido).
// É setado nos pontos que conhecem o pedido (create-order, consumer ao processar
// evento, relay ao publicar) e lido pelo handler para enriquecer TODOS os logs do fluxo.
type orderIDKey struct{}

// WithOrderID retorna um contexto anotado com o order_id (correlation_id de negócio).
// Logs emitidos com este contexto ganham order_id + correlation_id + trace_id/span_id
// automaticamente (ver ctxHandler).
func WithOrderID(ctx context.Context, orderID string) context.Context {
	if ctx == nil || orderID == "" {
		return ctx
	}
	return context.WithValue(ctx, orderIDKey{}, orderID)
}

// ctxHandler enriquece cada log com o contexto de correlação distribuída:
//   - order_id/correlation_id do fluxo (quando presente no context);
//   - trace_id/span_id do span OTel ativo (liga o log ao trace no Jaeger).
//
// Evita duplicar chaves que já vieram explicitamente na chamada do log.
type ctxHandler struct {
	next slog.Handler
}

func (h *ctxHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	orderID, _ := ctx.Value(orderIDKey{}).(string)
	if orderID != "" && !recordHas(r, "order_id") {
		r.AddAttrs(slog.String("order_id", orderID))
	}
	if orderID != "" && !recordHas(r, "correlation_id") {
		r.AddAttrs(slog.String("correlation_id", orderID))
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
		r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
	}
	return h.next.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ctxHandler{next: h.next.WithAttrs(attrs)}
}

func (h *ctxHandler) WithGroup(name string) slog.Handler {
	return &ctxHandler{next: h.next.WithGroup(name)}
}

func recordHas(r slog.Record, key string) bool {
	has := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			has = true
			return false
		}
		return true
	})
	return has
}

// Setup configura o logger global do serviço com saída JSON estruturada (log/slog),
// incluindo o nome do serviço como atributo em todos os registros. O handler também
// injeta automaticamente trace_id/span_id (OTel) e order_id/correlation_id quando o
// contexto de correlação estiver presente — chamar no início do main de cada componente.
func Setup(service string) {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(&ctxHandler{next: base}).With("service", service))
}
