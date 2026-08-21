# Fase 6: Métricas Prometheus + Dashboards Grafana

> Plano de execução. Consultar `PHASE_5_PLAN.md` (seção 8 — visão futura) e `EVOLUTION_PLAN.md`.

## Objetivo

Completar a observabilidade (hoje só temos **traces** no Jaeger) com **métricas**:
expor métricas Prometheus de cada serviço e visualizar em **dashboards Grafana**
(throughput, latência, backlog, outbox, sagas por estado).

## 1. Métricas a expor (por serviço)

| Métrica | Tipo | Labels | Exposta por | O que mede |
|---|---|---|---|---|
| `saga_events_received_total` | Counter | `service, event_type` | consumers (orchestrator, workers, projector) | eventos recebidos do Kafka |
| `saga_events_processed_total` | Counter | `service, event_type` | consumers | eventos processados com sucesso |
| `saga_events_failed_total` | Counter | `service, event_type` | consumers | eventos com erro (handler) |
| `saga_events_dlq_total` | Counter | `topic` | consumers | eventos movidos para a DLQ |
| `saga_process_duration_seconds` | Histogram | `service` | consumers | latência do handler (p50/p95/p99) |
| `saga_events_published_total` | Counter | `service, topic` | producer / relay | eventos publicados no Kafka |
| `saga_outbox_pending` | Gauge | — | outbox-relay | linhas pendentes na outbox |
| `saga_outbox_published_total` | Counter | — | outbox-relay | eventos publicados a partir da outbox |
| `saga_orders_pending` | Gauge | `status` | metrics-exporter | sagas em status intermediários (fila) |
| `saga_orders_completed_total` | Gauge | — | metrics-exporter | sagas COMPLETED |
| `saga_orders_failed_total` | Gauge | — | metrics-exporter | sagas FAILED |

**Explicação das escolhas**
- O **backlog do pipeline** é capturado por `saga_orders_pending` (sagas em `PAYMENT_PENDING`,
  `PAYMENT_APPROVED`, etc.) e `saga_outbox_pending` — proxies diretos do lag, sem depender da
  admin API do Kafka (que o kafka-go v0.4.51 não expõe).
- **Latência** via histograma → percentis no dashboard.
- **Erros/DLQ** para visão de saúde.

## 2. Instrumentação (código)

Novo pacote: `internal/infrastructure/metrics/metrics.go`
- Registra as métricas (`prometheus`/`promhttp` do `client_golang`).
- Helpers: `RecordReceived`, `RecordProcessed(service, eventType, duration)`,
  `RecordError`, `RecordDLQ(topic)`, `RecordPublished(service, topic)`,
  `SetOutboxPending(n)`, `RecordOutboxPublished()`.
- `MetricsServer(addr, service)` — sobe um HTTP server com `/metrics`.

Pontos de instrumentação:
- `consumer.consumeWorker`: received → process (timer) → processed/error; DLQ no `moveToDLQ`.
- `producer.Publish`/`PublishBatch`: `saga_events_published_total`.
- `outbox-relay`: a cada poll, `SetOutboxPending` (count pendente) e `RecordOutboxPublished`.
- **`cmd/metrics-exporter`** (novo): consulta o Postgres a cada 10s e expõe gauges
  `saga_orders_pending{status}`, `saga_orders_completed_total`, `saga_orders_failed_total`.

Portas dos `/metrics` (no docker-compose):
`orchestrator:9101`, `worker-payment:9102`, `worker-inventory:9103`,
`worker-notification:9104`, `projector:9105`, `outbox-relay:9106`, `metrics-exporter:9107`.

## 3. Infra (docker-compose)

- **Prometheus** (`prom/prometheus`, porta 9090) com `prometheus.yml` (scrape dos 7 endpoints).
- **Grafana** (`grafana/grafana`, porta 3000, admin/admin) com **provisionamento**:
  - datasource Prometheus (auto);
  - dashboard "Saga - Visão Geral" (JSON provisionado).
- Makefile: adicionar `prometheus` e `grafana` aos `SERVICES`.

## 4. Dashboard "Saga - Visão Geral" (painéis)

| Painel | Consulta (PromQL resumida) | Tipo |
|---|---|---|
| Eventos processados/s por serviço | `sum(rate(saga_events_processed_total[1m])) by (service)` | Time series |
| Latência p95 do handler | `histogram_quantile(0.95, sum(rate(saga_process_duration_seconds_bucket[1m])) by (le, service))` | Time series |
| Erros de processamento/s | `sum(rate(saga_events_failed_total[1m])) by (service)` | Time series |
| Eventos na DLQ | `sum(increase(saga_events_dlq_total[5m]))` | Stat |
| Sagas em fila (pendentes) | `saga_orders_pending` | Bar gauge |
| Sagas completadas / falhadas | `saga_orders_completed_total`, `saga_orders_failed_total` | Stat |
| Outbox pendente | `saga_outbox_pending` | Stat |
| Publicação da outbox/s | `rate(saga_outbox_published_total[1m])` | Time series |

## 5. Validação

1. `make check` verde.
2. `make up` sobe Prometheus + Grafana; targets verdes em `http://localhost:9090/targets`.
3. Load test (load-generator) → painéis refletem a carga (eventos/s, latência, outbox, sagas).
4. Screenshot/registro dos resultados no `BENCHMARK.md` (ou seção do README).

## 6. Fora de escopo

- Testcontainers (frente separada, próxima).
- Logs estruturados (Loki) — possível evolução.
- Alertas (Alertmanager) — possível evolução.
