# 📘 Manual Completo — Order Saga Microservices

> Documentação **detalhada** do projeto inteiro: arquitetura, estrutura de pastas/arquivos,
> tecnologias e motivações, fluxo da saga, modelo de dados, ambientes, comandos,
> observabilidade, **troubleshooting** (pod caindo, lag, outbox, DLQ...) e runbook operacional.
>
> **Leitura complementar:** `README.md` (visão geral + quick start), `EVOLUTION_PLAN.md`
> (roadmap) e `PHASE_*_PLAN.md` (planos/resultados por fase).

## Sumário

1. [Sobre o Projeto](#1-sobre-o-projeto)
2. [Arquitetura](#2-arquitetura)
3. [Estrutura de Pastas e Arquivos](#3-estrutura-de-pastas-e-arquivos)
4. [Tecnologias e Por Quê](#4-tecnologias-e-por-quê)
5. [Padrões de Arquitetura](#5-padrões-de-arquitetura)
6. [Fluxo de um Pedido e Máquina de Estados](#6-fluxo-de-um-pedido-e-máquina-de-estados)
7. [Modelo de Dados](#7-modelo-de-dados)
8. [Simuladores e Cenários Determinísticos](#8-simuladores-e-cenários-determinísticos)
9. [Ambientes de Execução](#9-ambientes-de-execução)
10. [Comandos Úteis](#10-comandos-úteis)
11. [Observabilidade](#11-observabilidade)
12. [Troubleshooting — Problemas Comuns e Como Resolver](#12-troubleshooting--problemas-comuns-e-como-resolver)
13. [Runbook Operacional](#13-runbook-operacional)
14. [Glossário](#14-glossário)
15. [Roadmap e Próximos Passos](#15-roadmap-e-próximos-passos)

---

## 1. Sobre o Projeto

Projeto de **estudo em Go** que simula o ciclo de vida de um **pedido** usando **saga
orquestrada** + **workers assíncronos via Kafka**, com persistência em PostgreSQL,
rastreabilidade, observabilidade, resiliência e **deploy em Kubernetes** — tudo "de
produção".

**O que ele demonstra na prática:**

- Coordenação central da saga (orquestrador) decidindo cada etapa e **compensando** quando
  algo falha.
- Comunicação assíncrona por eventos via Kafka (comandos e resultados em tópicos por etapa).
- **Transactional Outbox**: eventos decididos são gravados no banco e publicados por um
  relé, garantindo que "o que foi decidido" não se perde.
- **Transação atômica**: estado + journal + outbox em **um** `pgx.Tx` (Sem janelas residuais).
- **Idempotência** por `event_id` (at-least-once sem efeito duplicado).
- **DLQ** para mensagens com erro definitivo.
- **CQRS**: read model (`order_views`) alimentado por projeção via Kafka.
- **Observabilidade**: traces (OpenTelemetry/Jaeger), métricas (Prometheus/Grafana), logs
  correlacionados por `order_id`.
- **Resiliência**: retry com backoff, watchdog anti-stall, `restart: unless-stopped`,
  recuperação pós-restart.
- **CI/CD**: GitHub Actions (check, integração com Testcontainers, smoke e build de imagens
  multi-arch para GHCR).
- **Kubernetes**: Helm chart + KEDA escalando por lag do Kafka (validado em kind).

**Fluxo do negócio:** um pedido nasce em `PENDING`, passa por **pagamento → estoque →
notificação** e termina em `COMPLETED` (ou `FAILED`, com **estorno** de pagamento se o
estoque falhar após a aprovação).

---

## 2. Arquitetura

```
                    ┌──────────────────────────────────────────────────┐
                    │                    KAFKA                         │
                    │  orders.created / orders.payment / orders.inventory│
                    │  orders.notification / orders.status (+ .dlq)     │
                    └───▲──────┬──────────┬──────────┬──────────┬───────┘
        ORDER_CREATED │      │          │          │          │
   ┌──────────────────┴───┐  │          │          │          │
   │  create-order (CLI)  │  │ comando  │ comando   │ comando  │ ORDER_COMPLETED
   └──────────────────────┘  │          │          │          │ /ORDER_FAILED
                             ▼          ▼          ▼          ▼
   ┌──────────────┐    ┌────────────┐ ┌────────────┐ ┌────────────┐   ┌───────────┐
   │  orchestrator │───▶│ payment    │ │ inventory  │ │ notification│  │ order-status│
   │ (saga, tx)    │◀───│ worker     │◀│ worker     │◀│ worker      │  │ (auditoria) │
   └──────┬───────┘    └─────┬──────┘ └─────┬──────┘ └─────┬──────┘   └───────────┘
          │ estado+journal+outbox (1 TX)    │              │
          ▼                                 ▼              ▼
   ┌─────────────────────┐        ┌──────────────────────┐
   │ PostgreSQL (escrita) │        │  outbox-relay ──▶ Kafka (publica)
   │ sagas / saga_events  │        │  (claims SKIP LOCKED)
   │ outbox / processed   │        └──────────────────────┘
   └─────────────────────┘
   ┌─────────────────────┐        ┌──────────────────────┐
   │ PostgreSQL (leitura) │◀───────│  projector (CQRS)    │
   │ order_views          │        │  lê Kafka → read model│
   └─────────────────────┘        └──────────────────────┘

   Observabilidade: Jaeger (traces) · Prometheus (métricas) · Grafana (dashboards)
   Escala: SAGA_WORKERS + consumer groups · KEDA por lag (K8s)
```

**Componentes (processos):**

| Serviço | Papel |
|---|---|
| `create-order` | CLI que publica `ORDER_CREATED` (dispara uma saga) |
| `orchestrator` | Consome resultados, decide o próximo passo, persiste estado, compensa falhas |
| `worker-payment` | Processa `PAYMENT_COMMAND`/`PAYMENT_COMPENSATE` via simulador de gateway |
| `worker-inventory` | Processa `INVENTORY_COMMAND` (reserva de estoque) |
| `worker-notification` | Processa `NOTIFICATION_COMMAND` (notifica o cliente) |
| `outbox-relay` | Lê a tabela `outbox` e publica no Kafka (garante a entrega) |
| `projector` | Projeta eventos do Kafka no read model `order_views` (CQRS) |
| `order-status` | Consome eventos terminais (`ORDER_COMPLETED`/`ORDER_FAILED`) para auditoria externa |
| `metrics-exporter` | Expõe gauges do Postgres (sagas por status) para o Prometheus |
| `load-generator` | Publica N pedidos em lote (carga/benchmark) |
| `autoscaler` | (local) escala `docker-compose` por lag — análogo ao KEDA |

---

## 3. Estrutura de Pastas e Arquivos

```
.
├── cmd/                          # Pontos de entrada (1 main por processo)
│   ├── orchestrator/             # Orquestrador da saga
│   ├── worker-payment/           # Worker de pagamento (+ estorno)
│   ├── worker-inventory/         # Worker de estoque
│   ├── worker-notification/      # Worker de notificação
│   ├── outbox-relay/             # Relé outbox → Kafka
│   ├── projector/                # Projeção CQRS (read model)
│   ├── order-status/             # Auditoria de eventos terminais
│   ├── metrics-exporter/         # Gauges do Postgres p/ Prometheus
│   ├── create-order/             # CLI que publica ORDER_CREATED
│   ├── load-generator/           # Publica N pedidos (benchmark)
│   └── autoscaler/               # Escala compose por lag (análogo KEDA)
│
├── internal/
│   ├── application/              # Casos de uso (lógica de negócio)
│   │   ├── ports.go              # Interfaces (repositórios, gateways, SagaUnitOfWork)
│   │   ├── errors.go             # ErrNonRetryable (DLQ)
│   │   ├── orchestrator/         # Máquina de estados da saga (StartOrder/HandleResult)
│   │   ├── worker/               # PaymentUseCase, InventoryUseCase, NotificationUseCase
│   │   ├── projector/            # Projector (aplica eventos no read model)
│   │   └── domain/               # Entidades, status, evento, IDs
│   ├── domain/                   # Modelo de domínio (Saga, Event, OrderStatus)
│   ├── infrastructure/
│   │   ├── kafka/                # Producer, Consumer, DLQ, tópicos, env
│   │   ├── external/             # Simuladores de gateways + cenários determinísticos
│   │   ├── metrics/              # Métricas Prometheus + /healthz
│   │   ├── outbox/               # OutboxPublisher (EventPublisher → tabela outbox)
│   │   ├── persistence/
│   │   │   ├── postgres/         # SagaRepository, EventLogRepository, OutboxRepository
│   │   │   └── postgres_read/    # OrderViewRepository (read model)
│   │   ├── telemetry/            # OpenTelemetry (OTLP + W3C traceparent)
│   │   └── uow/                  # SagaUnitOfWork (transação atômica)
│   └── interfaces/logging.go     # Decorator de logs correlacionados
│
├── migrations/                   # SQL do banco de ESCRITA (golang-migrate)
│   ├── 000001_create_sagas       # tabela sagas
│   ├── 000002_create_saga_events # journal
│   ├── 000003_create_outbox      # outbox
│   ├── 000004_add_outbox_traceparent
│   └── 000005_add_outbox_claim   # claimed_at (SKIP LOCKED)
├── migrations-read/              # SQL do banco de LEITURA (order_views, processed_events)
│
├── deploy/
│   ├── helm/order-saga/          # Helm chart (Deployments, Services, ConfigMap, Secret,
│   │                             #   migrations Job, KEDA ScaledObjects, values*.yaml)
│   ├── k8s/                      # Manifests: kind-config, postgres, kafka(+init), Strimzi, bitnami
│   └── argocd/app.yaml           # Application GitOps (Fase 10)
│
├── scripts/
│   ├── benchmark.sh              # Cenário A/B/C do benchmark
│   └── k8s-smoke.sh              # Smoke end-to-end no cluster K8s
├── grafana/                      # Dashboard "Saga - Visão Geral" + provisioning
├── prometheus/                   # prometheus.yml + rules.yml (alertas)
├── .github/workflows/ci.yml      # CI: check, integration, smoke, build-images (GHCR)
├── terraform/                    # Fase 10: VPC + EKS + RDS (IaC)
├── docker-compose.yml            # Stack local (Kafka, Postgres, Jaeger, Grafana, app)
├── Makefile                      # Orquestra tudo (make up/check/k8s-up/aws-up...)
├── .env.example                  # Configuração por ambiente (12-factor)
└── *.md                          # README, EVOLUTION_PLAN, PHASE_*_PLAN, BENCHMARK
```

**Arquivos-chave para estudar primeiro:**

| Arquivo | Por que ler |
|---|---|
| `internal/application/ports.go` | Contrato do sistema (interfaces) |
| `internal/application/orchestrator/orchestrator.go` | Coração: máquina de estados da saga |
| `internal/application/worker/payment.go` | Exemplo completo de um worker (gateway + journal + outbox) |
| `internal/infrastructure/uow/unit_of_work.go` | Transação atômica (estado+journal+outbox) |
| `internal/infrastructure/kafka/consumer.go` | Consumer com retry, DLQ, watchdog anti-stall |
| `cmd/outbox-relay/main.go` | Relé da outbox (claims + publish em lote) |

---

## 4. Tecnologias e Por Quê

| Tecnologia | Por quê | Papel no projeto |
|---|---|---|
| **Go** | concorrência simples, binários pequenos (~10 MB), build rápido, ótimo para workers/CLIs | toda a aplicação |
| **Kafka (apache/kafka 3.9)** | barramento de eventos durável, ordenação por partição, consumer groups (escala horizontal) | transporte assíncrono |
| **kafka-go (segmentio)** | biblioteca madura p/ Kafka em Go, `GroupTopics`, leitura multi-tópico | producer/consumer |
| **PostgreSQL 16** | persistência com transações (ACID), JSONB p/ payloads | estado + journal + outbox + read model |
| **pgx/v5** | driver de alto desempenho; `pgx.Tx` para transação atômica | acesso ao banco |
| **Transactional Outbox** | garante que eventos decididos não se percam entre DB e Kafka | tabela `outbox` + `outbox-relay` |
| **OpenTelemetry + Jaeger** | traces distribuídos ponta a ponta (W3C `traceparent` via headers do Kafka) | observabilidade de fluxo |
| **Prometheus + Grafana** | métricas (throughput, latência, lag, outbox) + dashboards + alertas | observabilidade de números |
| **Testcontainers** | Kafka e Postgres REAIS em testes de integração (build tag `integration`) | qualidade |
| **Docker Compose** | stack local reproduzível (dev) | ambiente 1 |
| **kind + Helm + KEDA** | Kubernetes local; chart versionável; autoscaling por lag | ambiente 2 |
| **GitHub Actions + GHCR** | CI (check/integration/smoke) + imagens multi-arch | entrega contínua |
| **Terraform (Fase 10)** | VPC + EKS + RDS como código | cloud |

**Decisões de arquitetura notáveis:**

- **Saga orquestrada** (não coreografada): um orquestrador central decide cada passo e
  compensa falhas — mais fácil de auditar/rastrear (padrão microservices.io).
- **At-least-once + idempotência**: o Kafka pode reentregar; `event_id` único garante que
  reprocessar não duplica efeito.
- **Outbox + transação atômica**: estado + journal + outbox gravados no **mesmo** `pgx.Tx`
  (`SagaUnitOfWork`) → se a saga decidiu, o evento *existe* na outbox; se o evento não foi
  publicado, o relay tenta de novo (claims `FOR UPDATE SKIP LOCKED`).
- **CQRS**: banco de escrita (estado do fluxo) e banco de leitura (read model) separados,
  projetado via Kafka.

---

## 5. Padrões de Arquitetura

| Padrão | Implementação |
|---|---|
| **Saga orquestrada** | `orchestrator` decide cada etapa; workers executam; compensação (`PAYMENT_COMPENSATE`) quando o estoque falha após pagamento aprovado |
| **Transactional Outbox** | handlers gravam eventos na tabela `outbox` dentro da mesma transação; `outbox-relay` publica no Kafka e marca `published_at` |
| **Message Relay (polling)** | `ClaimPending` com `FOR UPDATE SKIP LOCKED` → múltiplos relays não duplicam |
| **Dead Letter Queue** | erros definitivos (`ErrNonRetryable`) vão para `orders.<etapa>.dlq` |
| **Idempotência** | UNIQUE em `event_id` (+ `(event_id, component, direction)` no journal) |
| **CQRS / Read Model** | `projector` consome Kafka e atualiza `order_views` (banco de leitura) |
| **Event Sourcing lite (journal)** | `saga_events` append-only com payloads de request/response = "trace de negócio" |
| **Transação atômica (Unit of Work)** | `SagaUnitOfWork`/`uow` → estado+journal+outbox em 1 `pgx.Tx` |
| **At-least-once** | Kafka redelivery + dedup por `event_id` |
| **Retry com backoff** | consumer: erros transitórios → retry com delay; definitivos → DLQ |
| **Watchdog anti-stall** | detecta kafka-go "travado" sem erro e **reconecta** o reader (≤45 s) |
| **Escala horizontal** | consumer groups + `SAGA_WORKERS` (goroutines); KEDA por lag no K8s |

---

## 6. Fluxo de um Pedido e Máquina de Estados

### 6.1 Passo a passo (caminho feliz)

```
1. create-order publica ORDER_CREATED        → tópico orders.created
2. orchestrator (StartOrder) salva PENDING,  → journal IN + outbox PAYMENT_COMMAND
   publica PAYMENT_COMMAND                   → tópico orders.payment.cmd
3. worker-payment (Process): aprovado?       → PAYMENT_RESULT → orders.payment.result
4. orchestrator (PAYMENT_APPROVED) salva,    → outbox INVENTORY_COMMAND → orders.inventory.cmd
5. worker-inventory (Reserve)                → INVENTORY_RESULT → orders.inventory.result
6. orchestrator (INVENTORY_RESERVED)         → outbox NOTIFICATION_COMMAND → orders.notification.cmd
7. worker-notification (Notify)              → NOTIFICATION_RESULT → orders.notification.result
8. orchestrator (NOTIFIED) → COMPLETED       → ORDER_COMPLETED → orders.status
9. projector atualiza order_views            → read model (CQRS)
10. order-status consome ORDER_COMPLETED     → auditoria externa
```

### 6.2 Máquina de estados (status em `internal/domain/status.go`)

As transições válidas são **explícitas em `internal/application/orchestrator/state_machine.go`
(`validTransitions`)**, validada por `canTransitionTo`/`assertTransition` em cada avanço —
um novo estado/transição aparece obrigatoriamente na tabela (review 3.2).

| Status | Significado | Como entra |
|---|---|---|
| `PENDING` | pedido criado, saga iniciada | `StartOrder` |
| `PAYMENT_PENDING` | aguardando pagamento | comando publicado |
| `PAYMENT_APPROVED` | pagamento ok | `PAYMENT_RESULT` aprovado |
| `PAYMENT_REFUND_PENDING` | estoque falhou → estorno | compensação iniciada |
| `PAYMENT_REFUNDED` | estorno concluído | `PAYMENT_COMPENSATE_RESULT` |
| `INVENTORY_RESERVED` | estoque reservado | `INVENTORY_RESULT` ok |
| `NOTIFIED` | notificação enviada | `NOTIFICATION_RESULT` ok |
| `COMPLETED` | saga concluída (terminal) | `ORDER_COMPLETED` publicado |
| `FAILED` | saga falhou (terminal) | `ORDER_FAILED` publicado |
| `RETRYING` | erro transitório → nova tentativa | resultado `RETRYING` |

Fluxo de transições (tabela `validTransitions`):

```
PENDING → PAYMENT_PENDING
PAYMENT_PENDING → PAYMENT_APPROVED | FAILED
PAYMENT_APPROVED → INVENTORY_RESERVED | PAYMENT_REFUND_PENDING | FAILED
PAYMENT_REFUND_PENDING → FAILED
INVENTORY_RESERVED → NOTIFIED | COMPLETED* | FAILED
NOTIFIED → COMPLETED
(* COMPLETED: falha de notificação ignorada — special-case)
```

### 6.3 Caminhos de erro

- **Gateway retorna erro transitório** (`error`) → worker publica `PAYMENT_RESULT(RETRYING)` →
  orquestrador `retry()` incrementa `retry_count` e **republica o comando** (até `maxRetries`,
  default 3). Esgotou → `FAILED`.
- **Gateway "recusa"** (`approved=false`, sem erro) → `FAILED` **imediato** (sem retry —
  recusa de negócio é definitiva).
- **Estoque falha após pagamento aprovado** → orquestrador inicia **compensação**
  (`PAYMENT_COMPENSATE` com `transaction_id`) → estorno → saga `FAILED` (metadado
  `payment_refund_failed=true`).
- **Notificação falha** → especial: mesmo com erro/retry esgotado, a saga **completa**
  (metadado `notification_error=true`) — notificação não derruba o pedido.

### 6.4 Regras de validação no orquestrador

- `expectedStatusForResult`: cada resultado só é aceito se a saga estiver na etapa esperada
  (evento fora de ordem → `ErrNonRetryable` → DLQ).
- `validateResultStatus`: status do resultado deve ser compatível com o evento.
- Idempotência: `eventLog.Has(event_id, "orchestrator")` → redelivery é ignorado.

---

## 7. Modelo de Dados

### 7.1 Banco de ESCRITA (`postgres`, db `saga`) — migrations/

| Tabela | Colunas principais | Papel |
|---|---|---|
| `sagas` | `order_id` (PK), `current_status`, `previous_status`, `retry_count`, `transaction_id` | estado corrente da saga |
| `saga_events` | `order_id`, `event_id`, `component`, `direction` (IN/OUT/GATEWAY_*), `status_anterior/atual`, `payload`, `request_payload`, `response_payload`, `created_at` | **journal** (append-only) — UNIQUE `(event_id, component, direction)` |
| `outbox` | `event_id` (UNIQUE), `topic`, `key`, `payload`, `traceparent`, `published_at`, `claimed_at` | eventos a publicar (relay) — índice parcial `WHERE published_at IS NULL` |

### 7.2 Banco de LEITURA (`postgres-read`, db `saga_read`) — migrations-read/

| Tabela | Colunas | Papel |
|---|---|---|
| `order_views` | `order_id`, `current_status`, `last_event_type`, `last_event_at`, `transaction_id`, `notification_error`, `payment_refund_failed`, `timeline` (JSONB) | read model (CQRS) — **estado terminal é final** (não regride) |
| `processed_events` | `event_id` (UNIQUE) | dedup do projector |

> **Consistência eventual:** o read model pode ficar um pouco atrás do banco de escrita
> (o projector consome do Kafka). Estado terminal (`COMPLETED`/`FAILED`) é **final** — um
> evento atrasado entra na timeline, mas não regride o status (fix da Etapa 7.4/7.5).

> 📖 **Aprofundamento:** cada tabela/coluna explicada (com SQL real, transação atômica,
> fluxo da saga pelas tabelas e consultas de diagnóstico) em [`DATABASE.md`](DATABASE.md).

---

## 8. Simuladores e Cenários Determinísticos

Os "gateways" são simulados (`internal/infrastructure/external/`) com taxas configuráveis:

| Gateway | Taxa de sucesso | Comportamento |
|---|---|---|
| `PaymentSimulator` | 0.85 (aprova) | `Process`/`Refund`; sucesso gera `tx-<order>-<nano>` |
| `InventorySimulator` | 0.90 (reserva) | `Reserve` |
| `NotificationSimulator` | 0.95 (envia) | `Notify` |

**Cenários determinísticos** (`scenario.go`): o `order_id` pode conter marcadores que
forçam comportamento — úteis para reproduzir falha/retry de forma previsível:

| Marcador no `order_id` | Efeito |
|---|---|
| `payment-fail` (ou `inventory-fail`, `notification-fail`) | gateway **recusa** sempre → saga `FAILED` (e estorno se pagamento aprovou) |
| `payment-retry` | gateway devolve **erro transitório** sempre → saga esgota retries → `FAILED` |
| `payment-retry-once` | erro na **1ª** tentativa, sucesso na 2ª → saga continua normalmente |

> Ex.: `make create-order ORDER_ID=order-payment-retry-once` demonstra o retry; o journal
> registra `RETRYING` → sucesso.

**Determinismo (review 2.3):** o sorteio aprovado/recusado é **determinístico por `order_id`**
(`randForOrder` = hash do orderID como seed) — o mesmo pedido decide igual em qualquer
instância do worker (consistente em scale horizontal). **Limitação documentada:** o cenário
`retry-once` conta tentativas em memória por instância; com múltiplas réplicas, o retry
pode cair em outra instância (aceitável para estudo — em produção o retry é do orquestrador,
que tem o `retry_count` persistido no banco).

---

## 9. Ambientes de Execução

O mesmo código (sem `if ambiente`) roda em 4 lugares — a configuração vem de variáveis
de ambiente (12-factor):

| Ambiente | Como sobe | Observabilidade |
|---|---|---|
| **Local (compose)** | `make up` | Jaeger, Prometheus, Grafana no compose |
| **Kubernetes local (kind)** | `make k8s-up` | métricas por pod (`/metrics`); KEDA |
| **Cloud AWS (EKS)** — Fase 10 | `make aws-up` (Terraform) | kube-prometheus-stack + dashboard |
| **CI (GitHub Actions)** | push/PR | logs do Actions; Testcontainers reais |

### 9.1 Configuração por ambiente (`.env`)

- **compose** lê do `.env` (credenciais, portas, tuning) e usa a rede interna
  (`kafka:9092`, `postgres:5432`).
- **host** (`go run`, testes, benchmark): `source .env` → `localhost:9094`, `localhost:5433`.
- **K8s**: `ConfigMap` + `Secret` via Helm (`values*.yaml`).
- O `.env.example` documenta tudo; o `.env` real é **gitignored**.

**Variáveis de tuning (defaults no `.env.example`):**

| Variável | Default | O que controla |
|---|---|---|
| `SAGA_WORKERS` | `1` | goroutines de consumo no mesmo group (concorrência intra-instância) |
| `KAFKA_COMMIT_BATCH` | `50` | mensagens antes do commit de offsets em lote |
| `KAFKA_COMMIT_INTERVAL` | `200ms` | intervalo máximo entre commits |
| `KAFKA_ACKS` | `all` | durabilidade do producer (`all` = leader+ISR; `one` = só leader, throughput) |
| `OUTBOX_BATCH_SIZE` | `2000` | lote do outbox-relay (maior = menos round-trips) |
| `OTEL_TRACES_SAMPLER` (+ `ARG`) | `parentbased_always_on` | amostragem de traces (produção: `parentbased_traceidratio` + `ARG=0.1`) |
| `GATEWAY_CB_ENABLED` / `_MAX_FAILURES` / `_TIMEOUT` | `true` / `5` / `10s` | circuit breaker dos gateways (fail-fast em falhas consecutivas) |
| `DATABASE_POOL_MAX_CONNS` / `_MIN_CONNS` / `_MAX_LIFETIME` / `_IDLE_TIMEOUT` | `10` / `2` / `1h` / `30s` | pool Postgres configurado (o default `max(4, NumCPU)` subdimensiona sob carga) |

### 9.2 Portas expostas (compose)

| Serviço | Host | Observação |
|---|---|---|
| Kafka | `localhost:9094` | brokers internos `kafka:9092` |
| Postgres escrita | `localhost:5433` | db `saga` |
| Postgres leitura | `localhost:5434` | db `saga_read` |
| Grafana | `localhost:3000` | admin/admin |
| Prometheus | `localhost:9090` | API de queries |
| Jaeger | `localhost:16686` | UI de traces |
| `/metrics` | 9101–9107 | orquestrador 9101, payment 9102, inventory 9103, notification 9104, projector 9105, relay 9106, metrics-exporter 9107 |

> No K8s os Services são internos (ClusterIP); use `kubectl port-forward` para acessar.

### 9.3 Tópicos Kafka (4 partições; DLQ com 1)

```
orders.created · orders.status
orders.payment.cmd · orders.payment.result
orders.inventory.cmd · orders.inventory.result
orders.notification.cmd · orders.notification.result
(cada tópico tem sua DLQ: <topic>.dlq)
```

> Tópicos **segregados por direção** (`.cmd` = comandos para os workers; `.result` =
> resultados para o orquestrador) — redução da amplificação de mensagens e contratos
> independentes (review 2.6/3.1).

---

## 10. Comandos Úteis

### 10.1 Makefile (o orquestrador de tudo)

```bash
make check          # fmt + build + vet + test + lint (qualidade)
make ci             # check + integration (Testcontainers)
make integration    # testes de integração (Kafka/Postgres reais)
make up             # sobe a stack local (compose)
make create-order ORDER_ID=order-001
make inspect ORDER_ID=order-001      # read model do pedido
make logs / make ps / make down
make autoscale                       # autoscaler local (lag → scale)
make rebuild                         # reconstrói as imagens
make k8s-up / k8s-down / k8s-logs SVC=orchestrator / k8s-smoke ORDER_ID=x
make aws-up / aws-down               # Fase 10 (Terraform)
```

### 10.2 Banco (psql)

```bash
# Estado das sagas por status
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT current_status, count(*) FROM sagas GROUP BY 1 ORDER BY 2 DESC;"

# "Trace de negócio" de um pedido (journal)
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT component, direction, event_type, status_anterior, status_atual, created_at
     FROM saga_events WHERE order_id='order-001' ORDER BY id;"

# Outbox pendente (relay atrasado?)
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT count(*) FROM outbox WHERE published_at IS NULL;"

# Read model
docker-compose exec postgres-read psql -U saga -d saga_read -c \
  "SELECT * FROM order_views WHERE order_id='order-001';"
```

### 10.3 Kafka (dentro do container)

```bash
# Lag do consumer group (fila do pipeline)
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
   --describe --group orchestrator'

# Ver mensagens da DLQ
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic orders.payment.result.dlq --from-beginning --max-messages 5'

# Listar tópicos / partições
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe'
```

### 10.4 Kubernetes (kind)

```bash
kubectl get pods -n order-saga -w
kubectl logs -n order-saga deploy/order-saga-orchestrator -f
kubectl get hpa -n order-saga                  # escalas do KEDA
kubectl get scaledobject -n order-saga
kubectl describe pod -n order-saga <pod>       # investigar CrashLoop/OOM
kubectl exec -n order-saga deploy/order-saga-postgres -- psql -U saga -d saga -c "..."
kubectl port-forward -n order-saga svc/order-saga-grafana 3000:3000
```

### 10.5 Prometheus (queries úteis)

```promql
# Throughput por serviço (eventos/s)
sum(rate(saga_events_processed_total[1m])) by (service)
# Lag por consumer group
saga_consumer_lag
# Outbox pendente
saga_outbox_pending
# Sagas por status (fila do pipeline)
saga_orders_pending
```

---

## 11. Observabilidade

### 11.1 Métricas (`internal/infrastructure/metrics/metrics.go`)

| Métrica | Tipo | O que mede |
|---|---|---|
| `saga_events_received_total` | counter | eventos recebidos (por serviço/tipo) |
| `saga_events_processed_total` | counter | eventos processados com sucesso |
| `saga_events_failed_total` | counter | eventos com erro no handler |
| `saga_events_dlq_total` | counter | eventos movidos para DLQ |
| `saga_process_duration_seconds` | histogram | latência do handler (p50/p95/p99) |
| `saga_outbox_pending` | gauge | linhas pendentes na outbox |
| `saga_outbox_published_total` | counter | publicados pelo relay |
| `saga_consumer_lag{group,topic}` | gauge | lag por consumer group |
| `saga_outbox_max_age_seconds` | gauge | idade do evento mais antigo não publicado |
| `saga_orders_pending{status}` / `saga_orders_completed_total` / `saga_orders_failed_total` | gauges | sagas por status (metrics-exporter) |

**Alertas** (`prometheus/rules.yml`): `SagaDLQGrowth` (DLQ crescendo) e
`SagaConsumerStalled` (sem progresso com lag).

**Probes de saúde (`/healthz`, portas 9101–9107):** cada serviço responde `200` somente com
conectividade real — `health.Postgres` (Ping/`SELECT 1`) e `health.Kafka` (dial+handshake);
o `outbox-relay` adiciona `health.LastActivity` (503 se o loop principal ficar sem concluir
um ciclo por 30s). Dependência inacessível → `503` com o motivo (usado nas probes do K8s).

### 11.2 Traces (OpenTelemetry + Jaeger)

- Cada evento consumido gera um span (`consume <EVENT_TYPE>`); o `traceparent` (W3C)
  atravessa o Kafka e a **outbox** (coluna `traceparent` reconstruída pelo relay).
- **Jaeger** (`localhost:16686`): buscar por `order_id` e ver o caminho completo
  orquestrador → worker → orquestrador.

### 11.3 Logs correlacionados

Todos os logs trazem `order_id`, `saga_id`, `event_id` e `phase`. Desde o hardening
(Fase 10) a saída é **JSON estruturado** (`log/slog`, handler JSON) — CloudWatch/Loki
leem sem parser customizado:

```json
{"time":"...","level":"INFO","msg":"despachando próximo comando","service":"orchestrator",
 "component":"orchestrator","phase":"decision","action":"dispatch-next",
 "order_id":"order-1","event_type":"PAYMENT_COMMAND","status_current":"PAYMENT_PENDING"}
{"time":"...","level":"INFO","msg":"evento processado","service":"worker-payment",
 "phase":"processed","event_id":"...","order_id":"order-1","type":"PAYMENT_RESULT",
 "duration":0.000024}
```

### 11.4 Dashboard "Saga - Visão Geral" (Grafana)

Painéis: throughput por serviço, latência (p50/p95), backlog (`saga_orders_pending`),
outbox pendente/idade, DLQ e lag por consumer group. Provisionado no compose
(`grafana/provisioning`); no K8s pode ser importado como JSON.

---

## 12. Troubleshooting — Problemas Comuns e Como Resolver

> **Ordem de investigação recomendada:** 1) logs do serviço → 2) estado da saga no banco →
> 3) lag do consumer group → 4) outbox → 5) recursos (CPU/memória).

### 12.1 Pod caindo / reiniciando (K8s)

| Sintoma | Causa provável | Como resolver |
|---|---|---|
| `CrashLoopBackOff` | app sai (panico, env inválido) | `kubectl logs -n order-saga <pod> --previous`; checar env (KAFKA_BROKERS/DATABASE_URL); subir o serviço dependente |
| `OOMKilled` | estourou o `limits.memory` | `kubectl describe pod` → `Last State: OOMKilled`; aumentar limits no `values.yaml`; reduzir concorrência (`SAGA_WORKERS`) |
| `ImagePullBackOff` | imagem não existe / privada | checar tag (`ghcr.io/<owner>/workers-kafka-<svc>:<tag>`); tornar pacote público ou criar `imagePullSecret` |
| `CreateContainerConfigError` | env/secret ausente | `kubectl describe pod` → detalhe do erro; conferir `ConfigMap`/`Secret` |
| `Pending` | PVC/sem recursos | `kubectl describe pod` → evento; checar `kubectl get pvc` (Bound?) e recursos do node |
| `Running 0/1` (readiness falhando) | probe `/healthz` não responde ou retorna `503` | `curl localhost:<porta>/healthz` no pod; `503` = dependência inacessível (Postgres/Kafka) — ver logs do componente; `404/erro de conexão` = serviço não subiu ou porta errada no `values.yaml` (ver §9.2) |
| reiniciando em loop (compose) | dependência não subiu ainda | `restart: unless-stopped` resolve sozinho; aguardar Kafka/Postgres `healthy` |

**Comandos:**
```bash
kubectl describe pod -n order-saga <pod>     # eventos + last state + image
kubectl logs -n order-saga <pod> --previous  # logs do crash anterior
kubectl rollout restart deployment/order-saga-orchestrator -n order-saga  # forçar restart limpo
```

### 12.2 Kafka/Postgres caindo (local)

| Sintoma | Causa | Resolver |
|---|---|---|
| Kafka `Exited (137)` / OOM | Colima sem memória (4 GB) | liberar memória (`docker-compose down` / `make k8s-down`); aumentar limit do kafka (1 Gi) |
| Kafka `ErrImagePull` TLS (quay.io) | ambiente não confia no quay.io | usar `apache/kafka` (Docker Hub) — foi o fallback adotado |
| Postgres sem espaço/`connection refused` | container caiu / portas em conflito | `make up`; trocar porta no `.env` |
| Tópicos somem após restart do kafka | dados não persistem (PVC/local-path) | re-rodar o Job `kafka-init` (tópicos são IaC) |
| Consumer groups com offset > log end | kafka reiniciou e perdeu dados | resetar grupos (`--reset-offsets --to-earliest`) ou reiniciar os pods da app |

### 12.3 Lag alto (pipeline atrás)

**Sintoma:** muitas sagas em `PAYMENT_PENDING` / `PAYMENT_APPROVED`; `saga_consumer_lag` alto;
`kafka-consumer-groups --describe` mostra LAG > 0.

**Causas e soluções:**

| Causa | Solução |
|---|---|
| Poucas réplicas / 1 consumer | escalar: `docker-compose up --scale orchestrator=3 ...` ou **KEDA** no K8s (sobe sozinho) |
| `outbox-relay` atrasado (outbox pendente) | relay já é ~485 ev/s; se atrás, adicionar relay (`--scale outbox-relay=2`) ou checar logs do relay |
| Postgres lentidão (transações) | checar `saga_orders_pending`; índice `idx_outbox_pending`/`saga_events(order_id, created_at)` já existem |
| Worker travado (stall do kafka-go) | **watchdog anti-stall** reconecta em ≤45 s (log `phase=stall-detected` → `phase=reconnect`); se recorrente, reiniciar o pod |
| Simulador com carga alta | reduzir volume ou aumentar `maxReplicas` no KEDA |

**Diagnóstico:**
```bash
# Quem está atrás?
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orchestrator'
# (repita para worker-payment/inventory/notification)

# Sagas presas em fila
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT current_status, count(*) FROM sagas GROUP BY 1 ORDER BY 2 DESC;"
```

### 12.4 Outbox crescendo / eventos não publicados

**Sintoma:** `saga_outbox_pending` alto / `saga_outbox_max_age_seconds` crescendo;
sagas decididas mas nada chega ao Kafka.

**Checar:**
```bash
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT count(*) FROM outbox WHERE published_at IS NULL;"
docker-compose logs outbox-relay | tail -50
```

**Causas:** relay parado (erro no publish → backoff), `MarkPublishedBatch` falhando,
broker inacessível (`KAFKA_BROKERS` errado). **Resolver:** corrigir o broker/relay; o relay
**reclama** pendências automaticamente no próximo ciclo (claims `SKIP LOCKED`); se ficou
órfão (claim antigo), o `claimTimeout` de 60 s libera a linha para outro relay.

> A **purga** remove eventos publicados há > 7 dias (`PurgePublished`) — a outbox não cresce
> para sempre.

### 12.5 DLQ acumulando

**Sintoma:** `saga_events_dlq_total` crescendo; mensagens em `orders.<etapa>.dlq`.

**Causas:** evento inválido / fora de ordem / status incompatível (erros **definitivos** —
`ErrNonRetryable`). Exemplos: `ORDER_CREATED` duplicado para saga já iniciada; resultado com
status inesperado; comando inválido.

**Inspecionar e reprocessar (o reprocessamento é seguro — idempotência):**
```bash
# Ver as mensagens da DLQ
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic orders.payment.result.dlq --from-beginning --max-messages 5'

# Reprocessar de volta para o tópico original
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic orders.payment.result.dlq --from-beginning | \
   /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
   --topic orders.payment.result'
```

> Se a causa for um **bug** (ex.: contrato de mensagem), corrija o código e faça deploy —
> a DLQ é o alerta; o replay é a ação.

### 12.6 Saga presa em status intermediário

**Sintoma:** saga parada em `PAYMENT_PENDING` / `PAYMENT_APPROVED` / `INVENTORY_RESERVED`
sem avançar.

**Diagnóstico em escada:**
```bash
# 1) A saga existe? em que estado?
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT order_id, current_status, retry_count, transaction_id FROM sagas WHERE order_id='X';"
# 2) O comando foi publicado? (outbox)
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT topic, published_at IS NULL AS pendente FROM outbox WHERE key='X';"
# 3) O worker da etapa está consumindo? (lag)
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group worker-payment'
# 4) Logs do worker/orquestrador
docker-compose logs worker-payment | tail -30
```

**Causas mais comuns:** consumer parado (stall → watchdog reconecta), worker caiu
(religar retoma), outbox pendente (relay), saga órfã por bug em versão antiga (limpeza manual).

**Resolver:** religar o serviço (`docker-compose restart worker-payment`); se ficou presa por
bug, pode-se **republicar o comando** manualmente (producer) ou, em último caso, ajustar o
estado no banco (estudo) — a idempotência evita duplicação.

### 12.7 Read model divergente / regressão de status

**Sintoma:** `order_views` com status diferente do banco de escrita; ou status "voltou"
(ex.: `COMPLETED` → `NOTIFIED`).

**Causa conhecida:** o projector consome **vários tópicos** com 1 consumer group; o Kafka
**não garante ordem entre tópicos** → um evento atrasado poderia regredir. **Já corrigido**
(Etapa 7.4/7.5): estado **terminal é final** — evento atrasado só entra na timeline.

**Se ainda houver divergência (sem ser terminal):** é consistência eventual (o projector
está atrás). Aguardar ou verificar o lag do group `projector`. Reprojeção total (estudo):
```bash
# (no kind) resetar o projector para reprojetar tudo
kubectl -n order-saga delete pod -l app.kubernetes.io/name=order-saga-projector
# ou truncar read model + resetar offsets do group projector (--to-earliest)
```

### 12.8 KEDA não escala (K8s)

| Sintoma | Causa | Resolver |
|---|---|---|
| `ScaledObject` READY=False | KEDA não alcança o Kafka | `bootstrapServers` deve ser o **FQDN** (`kafka.<ns>.svc.cluster.local:9092`); checar `kubectl logs -n keda -l app.kubernetes.io/name=keda-operator` |
| HPA `<unknown>` targets | scaler não lê lag | garantir consumer group com offsets; rodar carga (`load-generator`) |
| Não escala para baixo | cooldown (60 s) / lag ainda alto | aguardar `cooldownPeriod`; lag < threshold |
| Não escala para cima | lag < threshold | `lagThreshold` (200) é por partição; volume baixo não dispara |

### 12.9 Memória local (Colima 4 GB)

- **compose + kind juntos NÃO cabem** (Kafka, Postgres ×2, Jaeger, app...). Use um por vez:
  `make down` antes de `make k8s-up` (e vice-versa).
- Se o Kafka morrer com `137` (OOM): limite do kafka é 1 Gi; reduzir concorrência; subir
  stack com menos réplicas.
- Monitorar: `docker stats` (compose) / `kubectl top nodes` (kind).

### 12.10 Restart no meio do fluxo (resiliência)

O estado é **persistido** a cada passo e o consumo é at-least-once com idempotência:

- **Restart de qualquer serviço** no meio de uma saga → ela **continua de onde parou**
  (validado na Etapa 7.5: 1.000/1.000 sagas drenadas com restart do orquestrador).
- **Worker caído** → sagas ficam em espera; religar retoma (800/800 no teste R4).
- **Tópico deletado/recriado** → consumer **não morre** (`UnknownTopicOrPartition` = retry;
  watchdog reconecta). Mensagens em voo no tópico deletado são perdidas (inerente ao Kafka).
- **Relay duplicado** → `FOR UPDATE SKIP LOCKED` garante 1× por evento.

---

## 13. Runbook Operacional

### 13.1 Subir a stack (local)

```bash
cp .env.example .env        # opcional (credenciais/portas/tuning)
make up                     # infra + pipeline em background
make create-order ORDER_ID=order-001
make logs
```

### 13.2 Monitorar

| Ferramenta | URL | O quê |
|---|---|---|
| Grafana | http://localhost:3000 (admin/admin) | dashboard "Saga - Visão Geral" |
| Prometheus | http://localhost:9090 | métricas + alertas |
| Jaeger | http://localhost:16686 | traces por `order_id` |
| Postgres (escrita) | `localhost:5433` | `sagas`, `saga_events`, `outbox` |
| Postgres (leitura) | `localhost:5434` | `order_views` |

Lag por grupo:
```bash
docker-compose exec kafka /bin/sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orchestrator'
```

### 13.3 Diagnóstico rápido de um pedido

```bash
# Onde o pedido está no fluxo
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT order_id, current_status, retry_count, transaction_id, updated_at FROM sagas WHERE order_id='order-001'"

# "Trace de negócio" completo (journal)
docker-compose exec postgres psql -U saga -d saga -c \
  "SELECT component, direction, event_type, status_anterior, status_atual, created_at \
     FROM saga_events WHERE order_id='order-001' ORDER BY id"

# Read model
make inspect ORDER_ID=order-001
```

### 13.4 Recuperar DLQ (reprocessar mensagens)

Ver §12.5 (replay via console consumer/producer — seguro por idempotência).

### 13.5 Validar após mudanças

```bash
make check          # qualidade + unitários
make integration    # Testcontainers (Kafka + Postgres reais)
make ci             # check + integration
```

### 13.6 Kubernetes (kind)

```bash
make k8s-up                      # sobe cluster + Kafka + Postgres + Helm + KEDA
make k8s-smoke ORDER_ID=x        # smoke e2e (saga até terminal)
make k8s-down                    # derruba tudo
```

### 13.7 Cloud (Fase 10 — quando tiver credenciais)

```bash
brew install terraform awscli && aws configure
make aws-up        # terraform apply (VPC + EKS + RDS) ~15-20 min
make aws-down      # terraform destroy (custo ≈ zero quando parado)
```

---

## 14. Glossário

| Termo | Significado |
|---|---|
| **Saga** | sequência de transações distribuídas com compensação em caso de falha |
| **Orquestrador** | serviço central que decide o próximo passo da saga |
| **Worker** | executora de uma etapa (pagamento, estoque, notificação) |
| **Outbox** | tabela onde eventos decididos aguardam publicação (Transactional Outbox) |
| **Relay** | serviço que publica a outbox no Kafka e marca `published_at` |
| **DLQ** | Dead Letter Queue — tópico de erros definitivos |
| **Lag** | mensagens não consumidas de um tópico (backlog do consumer) |
| **Consumer group** | grupo de consumers que divide as partições (escala horizontal) |
| **Idempotência** | processar 2× tem o mesmo efeito de processar 1× (`event_id`) |
| **Journal** | `saga_events` — append-only; "trace de negócio" |
| **Read model** | `order_views` — projeção para consulta (CQRS) |
| **Unit of Work** | transação atômica (estado+journal+outbox em 1 `pgx.Tx`) |
| **Watchdog anti-stall** | mecanismo que reconecta o reader quando o kafka-go "trava" sem erro |
| **KEDA** | escalador do K8s por métricas externas (lag do Kafka) |
| **GHCR** | GitHub Container Registry (imagens Docker) |
| **Testcontainers** | containers reais (Kafka/Postgres) para testes de integração |

---

## 15. Roadmap e Próximos Passos

| Fase | Status | O que foi |
|---|---|---|
| 1 | ✅ | testes unitários + Testcontainers |
| 2 | ✅ | persistência, journal, read model |
| 3 | ✅ | outbox, DLQ, idempotência |
| 4 | ✅ | OpenTelemetry + Jaeger |
| 5 | ✅ | escala (partições, consumer groups, autoscaler) |
| 6 | ✅ | métricas Prometheus + Grafana |
| 7 | ✅ | performance + resiliência + **transação atômica** + validação final |
| 8 | ✅ | **CI/CD** (GitHub Actions → GHCR multi-arch) |
| 9 | ✅ | **Kubernetes local** (kind + Helm + KEDA) |
| 10 | 🚧 | **Cloud AWS** (Terraform/EKS/RDS/ArgoCD) — planejada; parte sem custo pronta |
| — | 📌 | API REST de consulta (read model pronto) |

> Detalhes por fase: `EVOLUTION_PLAN.md` e `PHASE_*_PLAN.md`.

---
*Fim do manual. Qualquer imprecisão, abra um issue/PR — o objetivo é aprender juntos.*
















