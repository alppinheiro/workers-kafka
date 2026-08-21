# Fase 2: Persistência, Rastreabilidade e Read Model

> Plano de execução aprovado em 20/08/2026. Fonte de verdade da fase — consultar junto
> com `instructions.md` e `EVOLUTION_PLAN.md`.

## Objetivo

1. **Eliminar a perda de estado do orquestrador** em restart (hoje o estado vive só no
   mapa em memória `o.states`).
2. **Rastreabilidade ponta a ponta**: todos os eventos de cada step (pedido, pagamento,
   estoque, notificação) salvos com payloads de **request/response** dos gateways, para
   ver como os dados foram enriquecidos (ex.: `transaction_id`) e diagnosticar onde está o problema.
3. **Read model de leitura** pronto para uma futura API REST de consulta.

## Decisões fechadas (histórico)

| # | Decisão |
|---|---|
| 1 | Banco de **escrita**: PostgreSQL 16 (`postgres:16-alpine`), driver `jackc/pgx/v5` (`pgxpool`). |
| 2 | Banco de **leitura**: segundo PostgreSQL 16, alimentado por **projeção via Kafka** (CQRS) — não replicação física, não dual-write em transação. |
| 3 | Migrations: **`golang-migrate/migrate`** via container `migrate/migrate` no `docker-compose`; diretórios `migrations/` (escrita) e `migrations-read/` (leitura). |
| 4 | Estado corrente em `sagas`; **todos os eventos** em `saga_events` (append-only) com `payload` + `request_payload`/`response_payload` dos gateways. |
| 5 | **Quem grava**: orquestrador grava eventos `IN`/`OUT`; workers grava `IN`/`OUT` + `GATEWAY_REQUEST`/`GATEWAY_RESPONSE` (request/response só existem nos workers). |
| 6 | Garantia de dados: **at-least-once + dedup por `event_id` + reentrega do Kafka**. Outbox Pattern adiado para a Fase 3. |
| 7 | Read model: `order_views` (denormalizada, 1 linha/pedido) + `processed_events` (dedup), escritos pelo novo serviço **`projector`**. |
| 8 | Consulta nesta fase: **SQL via psql** no banco de leitura. API REST e CLI `saga-inspect` ficam para depois. |
| 9 | Consistência eventual escrita→leitura é aceita (atraso de ms) e didática. |

## Arquitetura de dados

```
                     ┌───────────────────────────────┐
                     │            Kafka              │
                     │ orders.{created, payment,     │
                     │ inventory, notification,      │
                     │ status}                       │
                     └───────┬───────────────┬───────┘
       escritores            │               │ consumo de projeção
       (produtores)          │               ▼
                             │     ┌──────────────────────┐
  ┌──────────────────────┐   │     │ projector (novo cmd) │
  │ orchestrator         │   │     │ consumer group       │
  │ worker-payment       │   │     │ "projector"          │
  │ worker-inventory     │   │     └──────────┬───────────┘
  │ worker-notification  │   │                │
  └──────────┬───────────┘   │                ▼
             ▼               │     ┌──────────────────────────┐
  ┌────────────────────┐     │     │ postgres-read (LEITURA)  │
  │ postgres (ESCRITA) │     │     │  :5434  db=saga_read     │
  │  :5433  db=saga    │     │     │ order_views (read model) │
  │ sagas (estado)     │     │     │ processed_events (dedup) │
  │ saga_events        │     │     └──────────────────────────┘
  │ (journal+payloads) │     │                ▲
  └────────────────────┘     │    (futuro) API REST de consulta
                             │    de pedido (fora de escopo)
                             └── Kafka é o elo entre os bancos
```

- **Escrita** → `sagas` (decisão/recuperação) + `saga_events` (rastreabilidade).
- **Leitura** → `order_views` (consulta otimizada para a futura API).
- Nenhum componente grava nos dois bancos. A "interligação" é o Kafka.

## Modelo de dados — Migrations

### Escrita (`migrations/`)

`000001_create_sagas.up.sql` / `.down.sql`:

```sql
CREATE TABLE IF NOT EXISTS sagas (
    order_id        VARCHAR(255) PRIMARY KEY,
    saga_id         VARCHAR(255) NOT NULL,
    current_status  VARCHAR(50)  NOT NULL,
    previous_status VARCHAR(50),
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    transaction_id  VARCHAR(255),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

`000002_create_saga_events.up.sql` / `.down.sql`:

```sql
CREATE TABLE IF NOT EXISTS saga_events (
    id               BIGSERIAL PRIMARY KEY,
    order_id         VARCHAR(255) NOT NULL,
    saga_id          VARCHAR(255) NOT NULL,
    event_id         VARCHAR(255) NOT NULL,
    event_type       VARCHAR(50)  NOT NULL,
    component        VARCHAR(50)  NOT NULL,   -- orchestrator | worker-payment | ...
    direction        VARCHAR(20)  NOT NULL,   -- IN | OUT | GATEWAY_REQUEST | GATEWAY_RESPONSE
    status_anterior  VARCHAR(50),
    status_atual     VARCHAR(50),
    payload          JSONB,                   -- payload completo do evento no barramento
    request_payload  JSONB,                   -- payload enviado ao gateway externo
    response_payload JSONB,                   -- resposta do gateway externo
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (event_id, component, direction)  -- 1 visão por componente/evento/direção
);

CREATE INDEX IF NOT EXISTS idx_saga_events_order ON saga_events(order_id, created_at);
```

### Leitura (`migrations-read/`)

`000001_create_order_views.up.sql` / `.down.sql`:

```sql
CREATE TABLE IF NOT EXISTS order_views (
    order_id              VARCHAR(255) PRIMARY KEY,
    current_status        VARCHAR(50)  NOT NULL,
    last_event_type       VARCHAR(50),
    last_event_at         TIMESTAMPTZ,
    transaction_id        VARCHAR(255),
    notification_error    BOOLEAN NOT NULL DEFAULT false,
    payment_refund_failed BOOLEAN NOT NULL DEFAULT false,
    timeline              JSONB NOT NULL DEFAULT '[]',
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`000002_create_processed_events.up.sql` / `.down.sql`:

```sql
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Estrutura de código

**Novos arquivos:**

```
migrations/                                  # escrita
  000001_create_sagas.up.sql / .down.sql
  000002_create_saga_events.up.sql / .down.sql
migrations-read/                             # leitura
  000001_create_order_views.up.sql / .down.sql
  000002_create_processed_events.up.sql / .down.sql
internal/domain/saga.go                      # struct Saga (agregado persistido)
internal/application/projector/projector.go  # caso de uso do read model
internal/infrastructure/persistence/
  postgres/
    postgres.go              # Connect(ctx, url) com retry de ping
    env.go                   # DatabaseURLFromEnv()
    saga_repository.go       # Save (upsert) / Load
    event_log_repository.go  # Append
  postgres_read/
    env.go                   # ReadDatabaseURLFromEnv()
    order_view_repository.go # ApplyEvent + MarkProcessed (dedup)
cmd/projector/main.go        # consumer dos 5 tópicos + projeção
```

**Arquivos editados:**

```
internal/application/ports.go      # + SagaRepository, EventLogEntry, EventLogRepository,
                                   #   OrderViewRepository, ErrSagaNotFound
internal/application/orchestrator/orchestrator.go    # map → SagaRepository + EventLog
internal/application/orchestrator/{orchestrator_test.go, mocks_test.go}
internal/application/worker/{payment,inventory,notification}.go  # + EventLog
internal/application/worker/*_test.go
cmd/orchestrator/main.go
cmd/worker-{payment,inventory,notification}/main.go
docker-compose.yml
Makefile
.vscode/{launch.json,tasks.json}
go.mod / go.sum                  # + github.com/jackc/pgx/v5
instructions.md                  # atualizar decisões/escopo/histórico
EVOLUTION_PLAN.md                # marcar Fase 2
.github/copilot-instructions.md  # atualizar escopo
README.md                        # APÓS a implementação
```

### Portas novas (`internal/application/ports.go`)

```go
type SagaRepository interface {
    Save(ctx context.Context, saga domain.Saga) error
    Load(ctx context.Context, orderID string) (domain.Saga, error)
}

type EventLogEntry struct {
    OrderID         string
    SagaID          string
    EventID         string
    EventType       domain.EventType
    Component       string
    Direction       string // IN | OUT | GATEWAY_REQUEST | GATEWAY_RESPONSE
    StatusAnterior  domain.OrderStatus
    StatusAtual     domain.OrderStatus
    Payload         any
    RequestPayload  any
    ResponsePayload any
}

type EventLogRepository interface {
    Append(ctx context.Context, entry EventLogEntry) error
}

type OrderViewRepository interface {
    // ApplyEvent faz upsert em order_views a partir de um evento do barramento.
    ApplyEvent(ctx context.Context, event domain.Event) error
    // MarkProcessed registra o event_id; retorna false se já existia (dedup).
    MarkProcessed(ctx context.Context, eventID string) (bool, error)
}

var ErrSagaNotFound = errors.New("saga not found")
```

## Instrumentação por componente

### Orquestrador (`internal/application/orchestrator`)

- `New(publisher, sagaRepo, eventLog, maxRetries)`.
- Padrão em cada transição: `Load → Decide → Save(sagas) → AppendLog(OUT) → Publish`.
- `StartOrder`: `Load` → se `ErrSagaNotFound`, cria `Saga{Pending}` e `Save`; senão erro de
  saga duplicada (semântica atual preservada). Grava log `IN` do `ORDER_CREATED`.
- `HandleResult`: `Load` no início; helpers (`retry`, `fail`, `complete`,
  `startCompensation`, `dispatchNext`) recebem `*domain.Saga` e fazem `Save` + log `OUT`
  **antes** de cada `publisher.Publish`.
- `sagaState` privado vira `domain.Saga` (agregado persistido).

### Workers (payment / inventory / notification)

- Construtores ganham `eventLog application.EventLogRepository`.
- Ordem no processamento de um comando:
  1. log `IN` com `payload` do comando;
  2. `GATEWAY_REQUEST` com `request_payload` montado (ex.: `{order_id, op}`);
  3. chamada ao gateway;
  4. `GATEWAY_RESPONSE` com `response_payload` (ex.: `{approved, transaction_id}` ou `{error}`);
  5. log `OUT` com o `payload` do evento de resultado;
  6. `publisher.Publish(result)`.
- Se o `Append` falhar, retorna erro → mensagem não é commitada e será reentregue
  (coerente com at-least-once).

### Projector (`internal/application/projector` + `cmd/projector`)

- Consumer group `"projector"` nos tópicos `orders.created`, `orders.payment`,
  `orders.inventory`, `orders.notification`, `orders.status`.
- Handler:
  1. `MarkProcessed(event_id)` → `false` = evento já visto, retorna `nil` (dedup);
  2. `ApplyEvent(event)` → upsert em `order_views` (status, `transaction_id`, flags
     `notification_error`/`payment_refund_failed` de metadata, `timeline ||= <evento>`).
- Só conhece o banco de leitura.

## Garantias de dados (por que não há perda permanente)

| Ponto | Garantia |
|---|---|
| Grava no banco e morre antes do publish | Consumer commita **após** o handler; a reentrega do Kafka reprocessa e o dedup (`UNIQUE (event_id, component, direction)`) evita duplicar no `saga_events`. O resultado acaba publicado. |
| Publica no Kafka e morre antes do commit | Redelivery; `processed_events` no projector ignora. |
| Projector escreve e morre antes do commit | Redelivery; `MarkProcessed` é atômico (`INSERT ... ON CONFLICT DO NOTHING`). |
| Kafka RF=1 (docker-compose) | Limitação do ambiente de estudo. Produção: RF≥3 + `acks=all`. |
| Retenção de tópicos (default 7d) | Read model reconstruível via replay dos eventos. |

Regras invioláveis: (1) commit do Kafka somente após o handler; (2) dedup por `event_id`
em todos os escritores. Outbox Pattern (garantia mais forte) fica na Fase 3.

## Infra (docker-compose / Makefile / VS Code)

- Serviços novos: `postgres` (:5433), `migrations`, `postgres-read` (:5434), `migrations-read`, `projector`.
- `orchestrator` e workers: `DATABASE_URL=postgres://saga:saga@postgres:5432/saga?sslmode=disable`, `depends_on: migrations (service_completed_successfully)`.
- `projector`: `DATABASE_URL=postgres://saga:saga@postgres-read:5432/saga_read?sslmode=disable`, `depends_on: migrations-read`.
- Healthchecks: `pg_isready` nos dois Postgres.
- `Makefile`: `SERVICES` inclui os novos; alvo `inspect` para consultar o read model via psql.
- `.vscode/launch.json`: `DATABASE_URL=localhost:5433` (escrita) no orchestrator/workers; `localhost:5434` (leitura) no projector.
- `.vscode/tasks.json`: task de infra sobe `kafka kafka-init postgres migrations postgres-read migrations-read`.

## Testes

- Orquestrador: fake `SagaRepository` (mapa) + fake `EventLogRepository`; testes existentes
  que usavam `o.states` migram para `repo.Load(...)`. Cobertura deve permanecer alta.
- Workers: fakes do log; novos casos para `IN`/`OUT` e `GATEWAY_REQUEST`/`GATEWAY_RESPONSE`.
- Projector: fake `OrderViewRepository`; casos de dedup (`MarkProcessed=false`) e aplicação.
- Persistência Postgres: testes **guardados** (`t.Skip` se `DATABASE_URL` ausente) — ponte
  para os testes de integração com testcontainers (pendência da Fase 1).

## Ordem de execução (commits pequenos e validáveis)

1. **Domínio + portas**: `internal/domain/saga.go`, portas novas em `ports.go`.
   → `make build && make vet`
2. **Migrations (escrita)** + package `postgres` (Connect/env/repos).
   → subir `postgres` + `migrations` e conferir tabelas via psql
3. **Refactor do orquestrador** + atualização de testes (fake repo/log).
   → `make test`
4. **Refactor dos workers** + testes.
   → `make test`
5. **Wiring** de `cmd/orchestrator` e `cmd/worker-*` + `.vscode`.
   → `go build ./...`
6. **Migrations (leitura)** + package `postgres_read` + `OrderViewRepository`.
   → compose
7. **Projector**: caso de uso + `cmd/projector` + testes.
   → `make test`
8. **docker-compose + Makefile** completos.
   → `make up`
9. **Validação end-to-end**:
   - fluxo feliz e cenários de retry/falha (`make create-order ...`);
   - **teste de restart**: matar o orquestrador no meio de uma saga, subir de novo e
     conferir que a saga continua de onde parou (critério do `EVOLUTION_PLAN.md`);
   - conferir `order_views` no banco de leitura (timeline por `order_id`);
   - conferir `saga_events` no banco de escrita (payloads request/response).
10. **Documentação**: `instructions.md`, `EVOLUTION_PLAN.md`, `.github/copilot-instructions.md`.
11. **README.md** (após a implementação, conforme combinado).

## Fora de escopo nesta fase

- API REST de consulta de pedido (futuro — lê `order_views`).
- CLI `saga-inspect` (consultas via psql por enquanto).
- Outbox Pattern (Fase 3).
- DLQ / idempotência completa / retry avançado (Fase 3).
- Testes de integração com testcontainers (pendência da Fase 1 — opcional aqui).
- Concorrência com goroutines (Fase 5).

## Definição de Pronto

1. Orquestrador reinicia no meio de uma saga e ela continua de onde parou. ✅
2. Nenhuma mensagem é perdida (dedup + at-least-once nesta fase). ✅
3. Linha do tempo de qualquer `order_id` reconstruível na escrita e na leitura. ✅
4. Alterações validadas por testes. ✅
5. Performance/concorrência fica para a Fase 5.



