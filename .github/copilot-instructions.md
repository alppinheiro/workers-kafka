# Contexto e Instruções do Projeto: Order Saga Microservices (Go + Kafka)

## 1. Visão Geral do Projeto
Projeto de estudo em Go para simular o ciclo de vida de um pedido usando **Saga Orquestrada** e **workers assíncronos via Kafka**.

## 2. Escopo Atual e Restrições (Fase Atual)
- **Incluso (Fases 1–8 concluídas — ver `PHASE_*_PLAN.md`, `BENCHMARK.md` e `EVOLUTION_PLAN.md`):**
  - Testes unitários e de integração (Fase 1 + Testcontainers com Kafka e Postgres reais)
  - Persistência + rastreabilidade: `sagas`, `saga_events` (journal com payloads request/response)
  - Read model `order_views` (banco de leitura) projetado via Kafka pelo serviço `projector`
  - Outbox Pattern (`outbox` + `outbox-relay` com `FOR UPDATE SKIP LOCKED`), DLQ por tópico e
    idempotência por `event_id` (Fase 3)
  - Observabilidade distribuída: OpenTelemetry + Jaeger, propagação W3C `traceparent` via
    Kafka headers e outbox (Fase 4); métricas Prometheus por serviço + dashboard Grafana (Fase 6)
  - Escalabilidade: 4 partições, `SAGA_WORKERS` (goroutines no consumer), consumer groups
    multi-instância, autoscaler por lag (análogo ao KEDA/HPA) (Fase 5)
  - Performance/resiliência (Fase 7): relay da outbox em lote (~485 ev/s), commit de offsets em
    lote, watchdog anti-stall, rastreabilidade (lag por group, idade da outbox, alertas)
  - **Transação atômica única** (Etapa 7.4): estado + journal + outbox em um `pgx.Tx` via
    `SagaUnitOfWork` (`internal/infrastructure/uow`)
  - **CI/CD com GitHub Actions** (Fase 8): 4 jobs — `check`, `integration` (Testcontainers),
    `smoke` (e2e docker-compose) e `build-images` (9 imagens → GHCR) — pipeline verde validado
- **Fora de Escopo Nesta Fase:**
  - API REST de consulta de pedido
  - Escala horizontal real em Kubernetes (KEDA/HPA) e deploy em cloud — **Fases 9/10 planejadas**

## 3. Estrutura Obrigatória de Pacotes
```text
cmd/                          # Pontos de entrada (workers, orchestrator, projector, outbox-relay, autoscaler, load-generator, ...)
internal/domain/              # Entidades, status do pedido, enums e regras de negócio centrais
internal/application/         # Casos de uso, orquestrador da saga e coordenação
internal/infrastructure/
  ├── kafka/                  # Producer, consumer, tópicos, DLQ e configuração
  ├── external/               # Simuladores das APIs (Pagamento, Estoque, Notificação)
  ├── outbox/                 # OutboxPublisher (EventPublisher para a tabela outbox)
  ├── metrics/                # Métricas Prometheus expostas por serviço (Fase 6)
  ├── telemetry/              # OpenTelemetry (OTLP + propagação W3C traceparent)
  ├── uow/                    # SagaUnitOfWork — transação atômica estado+journal+outbox (7.4)
  └── persistence/
      ├── postgres/           # Banco de escrita: SagaRepository, EventLogRepository, OutboxRepository
      └── postgres_read/      # Banco de leitura: OrderViewRepository (read model)
internal/application/
  ├── orchestrator/           # Coordenação da saga (estado persistido)
  ├── worker/                 # Workers por etapa (logam request/response dos gateways)
  └── projector/              # Projeção de eventos Kafka no read model
internal/interfaces/          # Handlers, adapters e pontos de integração