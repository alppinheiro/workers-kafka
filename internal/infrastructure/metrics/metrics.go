package metrics

import (
	"log"
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
		Buckets: prometheus.DefBuckets,
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
)

func init() {
	prometheus.MustRegister(
		eventsReceived, eventsProcessed, eventsFailed, eventsDLQ,
		processDuration, eventsPublished,
		outboxPending, outboxPublished,
		ordersPending, ordersCompleted, ordersFailed,
		outboxMaxAge, consumerLag,
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

func SetOutboxMaxAge(seconds float64) {
	outboxMaxAge.Set(seconds)
}

func SetConsumerLag(group, topic string, n int64) {
	consumerLag.WithLabelValues(group, topic).Set(float64(n))
}

// Serve inicia (em goroutine) um servidor HTTP com /metrics na porta indicada.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("component=metrics addr=%s error=%v", addr, err)
		}
	}()
	log.Printf("component=metrics phase=started addr=%s", addr)
}
