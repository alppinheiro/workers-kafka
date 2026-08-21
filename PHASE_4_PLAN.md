# Fase 4: Observabilidade Distribuída (OpenTelemetry + Jaeger)

> Plano de execução da Fase 4. ✅ **Concluída e validada** — ver seção "Resultados" ao final.
> Consultar junto com `PHASE_2_PLAN.md`, `PHASE_3_PLAN.md`, `instructions.md` e `EVOLUTION_PLAN.md`.

## Objetivo

Rastrear um pedido em tempo real através de todos os componentes (orquestrador, workers, projector) usando traços distribuídos:

1. **Instrumentação OpenTelemetry**: SDK OTel em orquestrador, workers, projector, outbox-relay e create-order.
2. **Context Propagation via Kafka Headers**: o `trace_id` (W3C `traceparent`) atravessa o Kafka entre produtor e consumidores.
3. **Visualização com Jaeger**: container `jaegertracing/jaeger` no docker-compose, recebendo traces via OTLP.

## Decisões fechadas

| # | Decisão |
|---|---|
| 1 | Exporter **OTLP/HTTP** para o Jaeger (porta `4318`); env `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4318`). |
| 2 | Propagação **W3C Trace Context** (`traceparent`) via headers do Kafka. |
| 3 | Um span por evento consumido (`consume <EVENT_TYPE>`) com atributos `order_id` e `event_id`; producer injeta o contexto no header. |
| 4 | Jaeger UI em `http://localhost:16686`. |

## Como utilizar (resumo)

1. `make up` sobe o Jaeger junto com a stack.
2. `make create-order ORDER_ID=order-trace-001` dispara um pedido.
3. Abra `http://localhost:16686` → serviço `orchestrator` → busque `order-trace-001` → veja o grafo de spans (orchestrator → workers → projector).
4. Ou consulte via API: `curl http://localhost:16686/api/traces?service=orchestrator`.

## Estrutura de código

Novos arquivos:

```
internal/infrastructure/telemetry/telemetry.go   # Init(serviceName): TracerProvider OTLP + propagator
```

Editados:

```
internal/infrastructure/kafka/producer.go   # injeta traceparent nos headers
internal/infrastructure/kafka/consumer.go   # extrai traceparent + cria span por evento (ServiceName na config)
cmd/*/main.go                               # InitOTel + shutdown
docker-compose.yml / Makefile               # serviço jaeger + OTEL_EXPORTER_OTLP_ENDPOINT
```

## Validação

- `make check` (unitários continuam passando — spans noop sem OTel init nos testes).
- e2e: criar pedido e confirmar spans no Jaeger (UI/API).
- Docs: `README.md` (seção Observabilidade) + `instructions.md` (histórico).

## Fora de escopo nesta fase

- Métricas (Prometheus) e logs estruturados completos — podem ser evolução.
- Amostragem probabilística (usamos AlwaysSample para estudo).
- Concorrência/goroutines (Fase 5).

## Resultados

- **Validação e2e**: pedido `order-trace-003` gerou uma cadeia única de spans no Jaeger
  (`acbf0cd9...`) cobrindo `create-order → orchestrator → outbox-relay → worker-payment →
  projector` (e demais serviços), comprovando a propagação do `trace_id` via Kafka headers
  inclusive através da outbox.
- **Teste de carga (2000 pedidos)**: ingestão de ~47.000 eventos/s; `outbox-relay` foi
  otimizado para publicar em lote (`PublishBatch`), saindo de ~1 para centenas de eventos/s.
  O gargalo restante são os consumers single-threaded → **Fase 5 (concorrência)**.
