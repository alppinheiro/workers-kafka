package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	eventsReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_events_received_total",
		Help: "Eventos recebidos do Kafka por serviço e tipo.",
	}, []string{"service", "event_type"})

	eventsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_events_processed_total",
		Help: "Eventos processados com sucesso.",
	}, []string{"service", "event_type"})

	eventsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_events_failed_total",
		Help: "Eventos com erro no handler.",
	}, []string{"service", "event_type"})

	eventsDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_events_dlq_total",
		Help: "Eventos movidos para a DLQ por tópico.",
	}, []string{"topic"})

	processDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "saga_process_duration_seconds",
		Help:    "Latência do handler por serviço.",
		Buckets: processBuckets,
	}, []string{"service"})

	eventsPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_events_published_total",
		Help: "Eventos publicados no Kafka por serviço e tópico.",
	}, []string{"service", "topic"})

	outboxPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "saga_outbox_pending",
		Help: "Linhas pendentes na outbox.",
	})

	outboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "saga_outbox_published_total",
		Help: "Eventos publicados a partir da outbox.",
	})

	ordersPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_orders_pending",
		Help: "Sagas em status intermediários (fila do pipeline).",
	}, []string{"status"})

	// ordersByStatus expõe TODOS os status de sagas (incluindo terminais COMPLETED/FAILED e
	// os transientes com valor 0) para uma visão completa do estado atual no dashboard.
	ordersByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_orders_by_status",
		Help: "Sagas por status (visão completa: intermediários + terminais + zero).",
	}, []string{"status"})

	ordersCompleted = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "saga_orders_completed_total",
		Help: "Sagas COMPLETED.",
	})

	ordersFailed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "saga_orders_failed_total",
		Help: "Sagas FAILED.",
	})

	outboxMaxAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "saga_outbox_max_age_seconds",
		Help: "Idade (s) do evento mais antigo ainda não publicado na outbox.",
	})

	consumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_consumer_lag",
		Help: "Lag do consumer group por tópico (mensagens não processadas).",
	}, []string{"group", "topic"})

	// --- Métricas P0 (plano de observabilidade docs/OBSERVABILITY_PLAN.md) ----------

	ordersTerminal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_orders_terminal_total",
		Help: "Sagas encerradas em estado terminal (outcome=COMPLETED|FAILED). Counter incremental p/ rate() e success rate.",
	}, []string{"outcome"})

	sagaMaxAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_saga_max_age_seconds",
		Help: "Idade (s) da saga mais antiga ainda em status intermediário.",
	}, []string{"status"})

	dlqDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_dlq_depth",
		Help: "Quantidade de mensagens acumuladas em cada tópico de DLQ (fim do tópico).",
	}, []string{"topic"})

	consumerLastProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_consumer_last_progress_seconds",
		Help: "Tempo (s) desde o último progresso (fetch) do reader de cada serviço.",
	}, []string{"service"})

	consumerReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_consumer_reconnects_total",
		Help: "Reconexões do reader (watchdog anti-stall) por serviço.",
	}, []string{"service"})

	outboxGenerated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "saga_outbox_generated_total",
		Help: "Eventos registrados na outbox aguardando publicação (todas as publicações por outbox).",
	})
)

// processBuckets é mais fino que o DefBuckets do client_golang (começa em 5ms):
// os handlers do pipeline costumam terminar em poucos ms; buckets até 10s cobrem
// outliers/retries sem perder resolução no p50/p95.
var processBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10,
}

func init() {
	prometheus.MustRegister(
		eventsReceived, eventsProcessed, eventsFailed, eventsDLQ,
		processDuration, eventsPublished,
		outboxPending, outboxPublished,
		ordersPending, ordersCompleted, ordersFailed, ordersByStatus,
		outboxMaxAge, consumerLag,
		ordersTerminal, sagaMaxAge, dlqDepth,
		consumerLastProgress, consumerReconnects, outboxGenerated,
	)
}

// --- Helpers de gravação ------------------------------------------------------

func RecordReceived(service, eventType string) {
	eventsReceived.WithLabelValues(service, eventType).Inc()
}

func RecordProcessed(service, eventType string, duration time.Duration) {
	eventsProcessed.WithLabelValues(service, eventType).Inc()
	processDuration.WithLabelValues(service).Observe(duration.Seconds())
}

func RecordError(service, eventType string) {
	eventsFailed.WithLabelValues(service, eventType).Inc()
}

func RecordDLQ(topic string) {
	eventsDLQ.WithLabelValues(topic).Inc()
}

func RecordPublished(service, topic string) {
	eventsPublished.WithLabelValues(service, topic).Inc()
}

func SetOutboxPending(n int) {
	outboxPending.Set(float64(n))
}

func RecordOutboxPublished() {
	outboxPublished.Inc()
}

func SetOrdersPending(status string, n int) {
	ordersPending.WithLabelValues(status).Set(float64(n))
}

// ResetOrdersPending limpa todos os labels de status pendentes antes de um novo ciclo
// de coleta, evitando gauges "stale" (status que desapareceram mantendo o valor antigo).
func ResetOrdersPending() {
	ordersPending.Reset()
}

func SetOrdersCompleted(n int) {
	ordersCompleted.Set(float64(n))
}

func SetOrdersFailed(n int) {
	ordersFailed.Set(float64(n))
}

// SetOrdersByStatus atualiza a contagem de sagas em um status (visão completa: inclui
// terminais COMPLETED/FAILED e status transientes com valor 0, mantendo a série estável).
func SetOrdersByStatus(status string, n int) {
	ordersByStatus.WithLabelValues(status).Set(float64(n))
}

// ResetOrdersByStatus limpa todos os labels de status antes de um novo ciclo de coleta
// (anti-gauge-stale, mesmo comportamento do ResetOrdersPending).
func ResetOrdersByStatus() {
	ordersByStatus.Reset()
}

func SetOutboxMaxAge(seconds float64) {
	outboxMaxAge.Set(seconds)
}

func SetConsumerLag(group, topic string, n int64) {
	consumerLag.WithLabelValues(group, topic).Set(float64(n))
}

// RecordTerminal incrementa o contador de sagas encerradas (outcome: COMPLETED|FAILED).
func RecordTerminal(outcome string) {
	ordersTerminal.WithLabelValues(outcome).Inc()
}

// ResetSagaMaxAge limpa os labels de status antes de um novo ciclo de coleta
// (anti-gauge-stale, mesmo comportamento do ResetOrdersPending).
func ResetSagaMaxAge() {
	sagaMaxAge.Reset()
}

// SetSagaMaxAge atualiza a idade (s) da saga mais antiga em um status intermediário.
func SetSagaMaxAge(status string, seconds float64) {
	sagaMaxAge.WithLabelValues(status).Set(seconds)
}

// SetDLQDepth atualiza a quantidade de mensagens acumuladas em um tópico de DLQ.
func SetDLQDepth(topic string, n int64) {
	dlqDepth.WithLabelValues(topic).Set(float64(n))
}

// SetConsumerLastProgress atualiza o tempo desde o último progresso (fetch) do reader.
func SetConsumerLastProgress(service string, seconds float64) {
	consumerLastProgress.WithLabelValues(service).Set(seconds)
}

// RecordConsumerReconnect incrementa o contador de reconexões do reader (watchdog).
func RecordConsumerReconnect(service string) {
	consumerReconnects.WithLabelValues(service).Inc()
}

// RecordOutboxGenerated incrementa o contador de eventos registrados na outbox.
func RecordOutboxGenerated() {
	outboxGenerated.Inc()
}

// Serve inicia (em goroutine) um servidor HTTP com /metrics e /healthz na porta indicada.
// O /healthz permite liveness/readiness probes no Kubernetes (Fase 9) sem expor outro
// endpoint de health nos serviços. Comportamento histórico: responde 200 incondicional.
func Serve(addr string) {
	ServeWithChecks(addr)
}

// HealthCheck verifica a saúde de uma dependência; retorna erro quando indisponível.
type HealthCheck func(ctx context.Context) error

// healthTimeout limita cada check do /healthz para a probe nunca travar o servidor.
const healthTimeout = 3 * time.Second

// ServeWithChecks inicia o servidor HTTP de /metrics e /healthz. Quando checks são
// fornecidas, o /healthz executa cada uma (com timeout) e responde 503 se qualquer
// uma falhar — readiness real (conectividade Kafka/Postgres, stall de loop) em vez
// do 200 incondicional.
func ServeWithChecks(addr string, checks ...func(ctx context.Context) error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		for _, check := range checks {
			checkCtx, cancel := context.WithTimeout(r.Context(), healthTimeout)
			err := check(checkCtx)
			cancel()
			if err != nil {
				slog.Error("healthz falhou", "component", "healthz", "phase", "failed", "addr", addr, "error", err)
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("servidor de métricas encerrou com erro", "component", "metrics", "addr", addr, "error", err)
		}
	}()
	slog.Info("métricas iniciadas", "component", "metrics", "phase", "started", "addr", addr)
}
