# 📊 Observabilidade — Plano de Evolução (Grafana + Prometheus + Traces)

> **Status:** planejamento aprovado em 03/09/2026. **Fases A e C concluídas** (métricas P0 +
> buckets finos + docs) e **parte da Fase B** (regras novas em `prometheus/rules.yml`;
> Alertmanager/notificação pendente). Dashboards: D0 enriquecido (14 painéis) + D1 `saga-flow`,
> D2 `kafka-consumers`, D3 `saga-outbox`, D4 `saga-postgres`, D5 `saga-infra`.
> **Fase D (fix OTLP): concluída no kind** — Jaeger all-in-one (`deploy/k8s/otel.yaml`)
> atrás do Service `order-saga-otel:4318` (endpoint que o chart já esperava; eliminado o
> erro de export) + datasource Jaeger no Grafana (compose). Pendente: spans P1 de etapa/
> relay e `create-order` como raiz. Próximas: **Fase E (Loki)** e B restante.
> Escopo: enriquecer o monitoramento do fluxo de saga (orquestrador + workers + projector +
> outbox-relay) com dashboards acionáveis, novas métricas, alertas/SLO e correlação
> métrica ↔ trace ↔ log, válido para docker-compose e Kubernetes (kind/EKS).

## 1. Estado atual (inventário)

### 1.1 Métricas expostas por serviço (`internal/infrastructure/metrics/metrics.go`, portas 9101–9107)

Todos os serviços (exceto `order-status`) expõem `/metrics` e `/healthz` com checks reais.

| Métrica | Tipo | Labels |
|---|---|---|
| `saga_events_received_total` | counter | service, event_type |
| `saga_events_processed_total` | counter | service, event_type |
| `saga_events_failed_total` | counter | service, event_type |
| `saga_events_dlq_total` | counter | topic |
| `saga_process_duration_seconds` | histogram (DefBuckets) | service |
| `saga_events_published_total` | counter | service, topic |
| `saga_outbox_pending` | gauge | – |
| `saga_outbox_published_total` | counter | – |
| `saga_consumer_lag` | gauge | group, topic |
| `saga_outbox_max_age_seconds` | gauge | – |
| `saga_orders_pending{status}` / `saga_orders_completed_total` / `saga_orders_failed_total` | gauges | status / – / – |

Há ainda métricas automáticas do Go (`go_*`, `process_*`, `process_start_time_seconds`) e `up`
dos targets do Prometheus.

**Fontes:** cada serviço expõe `/metrics`; o `metrics-exporter` (9107) lê Postgres (sagas por
status, idade da outbox) e Kafka (`ListOffsets`/`OffsetFetch`) a cada 10s.

### 1.2 Stack de observabilidade

- **docker-compose:** Prometheus (scrape 5s, `prometheus/prometheus.yml` + `rules.yml`),
  Grafana (datasource provisionado com `uid: Prometheus`, dashboard "Saga - Visão Geral" com
  10 painéis em `grafana/dashboards/saga-overview.json`), Jaeger (OTLP 4318, UI 16686).
- **Kubernetes (kind):** métricas por pod (`/metrics`) e KEDA; **sem Prometheus/Grafana/Jaeger**
  (item 9.7 adiado) e o endpoint OTLP aponta para `order-saga-otel:4318` que **não existe**
  (erros de export nos logs).
- **Traces:** OTel por evento (`consume <EVENT_TYPE>`, attrs `order_id`/`event_id`, W3C
  `traceparent` via headers Kafka/outbox), exporter OTLP/HTTP.
- **Alertas:** `prometheus/rules.yml` com `SagaDLQGrowth` e `SagaConsumerStalled` (sem
  Alertmanager/notificação).

## 2. Gaps identificados

| # | Gap | Impacto |
|---|---|---|
| G1 | Apenas 1 dashboard, sem template vars, thresholds, heatmap, drill-down | Navegação e leitura visual pobres |
| G2 | Sem métricas de terminal/retries/idade por etapa → sem success rate/SLO/"pedido preso" | Falta o indicador principal de negócio |
| G3 | Dashboard não usa `received`, `published`, `go_*`, `process_*`, `up` | Painéis RED/Infra incompletos |
| G4 | Alertas sem notificação; `rate == 0` é frágil | Alarme silencioso/falso-negativo |
| G5 | kind sem stack de observabilidade e endpoint OTel fantasma | Sem rastreabilidade/métricas no cluster |
| G6 | Kafka broker e Postgres sem exporters (JMX) | Infra invisível (custo alto p/ estudo) |
| G7 | Logs sem coleta central (Loki) | Análise pós-incidente lenta |
| G8 | Nomenclatura `..._total` em *gauges* acumulados (`orders_completed_total`) | Impede `rate()`/SLO |

## 3. Princípios

- Modelos **RED** (Rate/Erros/Duração), **USE** (Utilização/Saturação/Erros) e visão de **SLO**.
- Métricas agregáveis, cardinalidade baixa (nunca por `order_id`/`event_id`); isso é papel de traces.
- Sufixos padronizados (`_total`, `_seconds`, `_age_seconds`); histogramas com buckets adequados.
- Dashboards e regras versionados, provisionados e validados (lint/API/carga).
- Alertas com severidade, runbook e notificação testável.

## 4. Novas métricas propostas

### P0 (pré-requisito de SLO/dashboards de negócio)

| Métrica | Tipo | Labels | Origem sugerida |
|---|---|---|---|
| `saga_orders_terminal_total` | counter | `outcome=COMPLETED\|FAILED` | orquestrador ao decidir terminal |
| `saga_saga_max_age_seconds` | gauge | `status` | metrics-exporter (pedido mais antigo por status) |
| `saga_dlq_depth` | gauge | `topic` | metrics-exporter (`ListOffsets` nos tópicos `.dlq`) |
| `saga_consumer_last_progress_seconds` | gauge | `service` | consumer (watchdog `lastProgress`) |
| `saga_consumer_reconnects_total` | counter | `service` | consumer (ponto do `reconnect`) |
| `saga_outbox_generated_total` | counter | – | outbox `Publisher` (toda gravação na outbox; a persistência não registra o serviço → contador global) |

### P1

| Métrica | Tipo | Labels | Origem sugerida |
|---|---|---|---|
| `saga_end_to_end_duration_seconds` | histogram | outcome | orquestrador (created_at → terminal) |
| `saga_orders_retries_total` | counter | service/event_type | handlers/orquestrador |
| `saga_consumer_lag_partition` | gauge | group, topic, partition | metrics-exporter |
| `saga_topic_latest_offset` | gauge | topic, partition | metrics-exporter |
| buckets finos no `saga_process_duration_seconds` | – | – | metrics.go (1ms..10s) |

### P2 (opcional — custo/recursos)

- `kafka_exporter` (JMX) e `postgres_exporter`.
- `saga_consumer_reader_*` expondo `reader.Stats()` (dials, rebalances, errors, fetches).
- Exemplars métrica↔trace.

## 5. Dashboards propostos (`grafana/dashboards/*.json`)

- **D0** `Saga - Visão Geral` (enriquecer): template vars (`service/status/group/topic/event_type`),
  thresholds/cores, stat de resumo, usar `received`/`published`/`up`, drill-down.
- **D1** `Fluxo do Pedido`: pedidos criados/s, success rate, time-to-terminal (P1), fila por
  status + idade do mais antigo, retries, sagas presas.
- **D2** `Kafka & Consumers`: lag por grupo/tópico, consumo vs produção, `last_progress`
  (stall), `reconnects`, DLQ (taxa + depth), hot partition (P1).
- **D3** `Outbox & Durabilidade`: pendente, max age (limiar), gerado vs publicado, taxa do relay.
- **D4** `PostgreSQL & Sagas`: status/terminal/taxa terminal (P0), retries, distribuição de duração (P1).
- **D5** `Infra & Recursos`: `up`, memória/CPU/goroutines/GC por serviço, restarts.
- **D6** `Rastreabilidade`: datasource Jaeger/Tempo, trace por `order_id`.
- **D7** (opcional) `Logs`: Loki por `order_id`/`trace_id`.

## 6. Alertas e SLO

Reorganizar regras (`rules.yml`) + **Alertmanager** (webhook/Slack/email). Novas regras:

`SagaStuckInStatus` (age por status > 30s), `OutboxAging` (max age > 30s), `DLQDepth` (depth > 0),
`ConsumerNoProgress` (last_progress > 60s com lag), `ConsumerReconnectLoop`
(`increase(reconnects[5m]) > 3`), `ErrorBudgetBurn` (success rate < 95% em 10m),
`ServiceDown` (`up == 0`), `ServiceRestart` (`changes(process_start_time_seconds[10m]) > 0`).

SLOs de estudo: ≥99% das sagas termina em <10s; outbox drena <10s sob carga; lag <200 fora de
pico; DLQ sem crescimento.

## 7. Traces

- **Fix no kind:** criar destino OTLP (Jaeger all-in-one ou OTel Collector) e corrigir
  `values.otel.endpoint` (remover `order-saga-otel` fantasma).
- Sampling por ambiente (`parentbased_always_on` local; `parentbased_traceidratio` 10% sob carga).
- Spans de etapa nos workers (gateway, scenario, attempt), span de `claim/publish` no relay e
  `create-order` como raiz. Atributos padronizados.
- Exemplars métrica↔trace (P2).

## 8. Logs

- Logs JSON já têm `service`/`order_id`; **adicionar** `trace_id`/`span_id`.
- P1: Loki + dashboard D7; até lá, greps canônicos documentados.

## 9. Fases de execução

| Fase | Escopo | Entrega |
|---|---|---|
| **A — Métricas P0** | novos counters/gauges + buckets finos + nomenclatura (sem quebrar dashboard) | métricas P0 no `/metrics`, docs atualizadas |
| **B — Alertas/SLO + Alertmanager** | regras novas agrupadas + notificação | alertas "de verdade" |
| **C — Dashboards D0–D5** | enriquecer D0 + D1..D5 provisionados | suíte de painéis |
| **D — Traces no kind + fix endpoint** | Jaeger/Collector + values + spans P1 | rastreabilidade no cluster |
| **E — Logs (Loki) + trace_id** | Loki + D7 | correlação fim-a-fim |
| **F — (Opcional)** | exporters Kafka/Postgres, exemplars, kube-prometheus-stack | monitor de "produção" |

DoD por fase: `go build/vet/test` verdes, `promtool check rules`, dashboards validados via API
do Grafana com carga (`make up` + load-generator/smoke), alertas testados (disparo/resolução),
docs atualizadas (MANUAL/README/docs/KUBERNETES.md).

## 10. Critérios de aceite

1. Em 30s responder: quem está atrás, o que falha, há quanto tempo um pedido está preso, % de sucesso.
2. Painel de negócio com success rate + p95 E2E + pedidos presos.
3. Problema real (stall, DLQ, outbox velha) aparece em métrica + alerta + painel antes dos logs.
4. Mesmo dashboard funciona no compose e no kind (mesmos `/metrics`).
5. Zero ruído (sem endpoint OTel fantasma); métricas documentadas e versionadas.

## 11. Recomendação de prioridade

**Fase A + C primeiro** (retorno visual imediato), depois **B** (alertas) e **D/E** (traces/logs).


