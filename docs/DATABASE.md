# 🗄️ Banco de Dados — Detalhamento

> **Por que este documento existe:** o projeto usa **2 bancos PostgreSQL** e **5 tabelas**.
> Aqui explicamos, para cada tabela e coluna, **o porquê** — não só o que é, mas qual problema
> de design/consistência ela resolve. SQL real das `migrations/`.

## 1. Visão Geral — Por que DOIS bancos?

O projeto aplica **CQRS** (Command Query Responsibility Segregation): o fluxo de escrita e a
consulta são separados.

| Banco | Tabelas | Quem escreve | Quem lê |
|---|---|---|---|
| **Escrita** (`postgres`, db `saga`) | `sagas`, `saga_events`, `outbox` | orquestrador e workers (em **uma transação**) | o próprio fluxo + `metrics-exporter` |
| **Leitura** (`postgres-read`, db `saga_read`) | `order_views`, `processed_events` | apenas o **`projector`** (via Kafka) | consultas/read model |

**Por quê?**
- **Isolamento de carga:** a escrita tem transações e locks; a leitura é otimizada para
  consulta — não competem pelo mesmo banco.
- **Modelo sob medida:** o read model (`order_views`) é desenhado **para quem pergunta**
  ("qual o status do pedido?"), em vez de exigir joins na máquina de estados.
- **Consistência eventual:** o `projector` consome os eventos do Kafka e atualiza o read
  model — o banco de escrita é a fonte da verdade; o de leitura é uma projeção.

---

## 2. Banco de ESCRITA (`saga`)

### 2.1 `sagas` — o estado corrente da saga

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

**Por que existe:** sem persistência, um restart do orquestrador **perderia** a saga no meio
do fluxo. Essa tabela garante a **recuperação pós-restart**: o orquestrador, ao consumir um
evento, carrega `sagas` e continua de onde parou.

**Cada coluna, por quê:**

| Coluna | Por quê |
|---|---|
| `order_id` (PK) | o pedido é a chave natural; todo evento/correlação usa `order_id` (partição no Kafka) |
| `saga_id` | identifica a saga (aqui igual ao `order_id`; deixa espaço para sagas compostas) |
| `current_status` | o **status atual** da máquina de estados (a decisão de negócio) |
| `previous_status` | o **status anterior** — essencial para o journal/auditoria e para a compensação saber de onde veio |
| `retry_count` | quantas vezes a etapa atual já foi retentada — o orquestrador decide `retry` vs `FAILED` comparando com `maxRetries` |
| `transaction_id` | **o ID do pagamento aprovado** — sem ele não dá para estornar (compensação) quando o estoque falha depois |
| `updated_at` | rastreio de quando mudou |

### 2.2 `saga_events` — o journal (append-only)

```sql
CREATE TABLE IF NOT EXISTS saga_events (
    id               BIGSERIAL PRIMARY KEY,
    order_id         VARCHAR(255) NOT NULL,
    saga_id          VARCHAR(255) NOT NULL,
    event_id         VARCHAR(255) NOT NULL,
    event_type       VARCHAR(50)  NOT NULL,
    component        VARCHAR(50)  NOT NULL,
    direction        VARCHAR(20)  NOT NULL,
    status_anterior  VARCHAR(50),
    status_atual     VARCHAR(50),
    payload          JSONB,
    request_payload  JSONB,
    response_payload JSONB,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (event_id, component, direction)
);
CREATE INDEX IF NOT EXISTS idx_saga_events_order ON saga_events(order_id, created_at);
```

**Por que existe:** é o **"trace de negócio"** — um registro **imutável** (só INSERT) de TUDO
que aconteceu com o pedido: eventos consumidos, comandos publicados e as **requisições e
respostas** trocadas com os gateways. Complementa o trace técnico do Jaeger com o *payload
de negócio*.

**Cada coluna/design, por quê:**

| Item | Por quê |
|---|---|
| `direction` | qual visão do evento: `IN` (consumido), `OUT` (publicado), `GATEWAY_REQUEST`/`GATEWAY_RESPONSE` (chamadas externas) |
| `component` | quem registrou (`orchestrator`, `worker-payment`, ...) |
| `UNIQUE (event_id, component, direction)` | **idempotência**: se a mensagem for reentregue pelo Kafka, o INSERT conflita e é ignorado (`ON CONFLICT DO NOTHING`) → 1 visão por componente/evento/direção |
| `payload` / `request_payload` / `response_payload` (JSONB) | auditabilidade total: o que chegou, o que foi enviado ao gateway e o que voltou |
| `status_anterior`/`status_atual` | a transição de estado que este evento representou |
| `idx_saga_events_order (order_id, created_at)` | **correlação rápida** — "conta a história" de um pedido em ordem (consultas de auditoria) |

> É a base do **debug**: `SELECT ... FROM saga_events WHERE order_id='X' ORDER BY id` mostra
> o fluxo inteiro do pedido, inclusive onde falhou e por quê.

### 2.3 `outbox` — o Transactional Outbox

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     VARCHAR(255) NOT NULL UNIQUE,
    topic        VARCHAR(100) NOT NULL,
    key          VARCHAR(255) NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);  -- migration 000004: + traceparent VARCHAR(64)
    -- migration 000005: + claimed_at  TIMESTAMPTZ

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(published_at, id) WHERE published_at IS NULL;
```

**Por que existe (o problema que resolve):** publicar evento no Kafka **dentro** da transação
não é atômico (o Kafka não participa da tx). Se o evento fosse publicado antes do commit e o
commit falhasse, teríamos um "evento fantasma". Se fosse depois e o processo morresse,
perderíamos o evento. O padrão **Transactional Outbox** resolve: o evento é gravado na
`outbox` **na mesma transação** do estado; um **relé** (`outbox-relay`) lê e publica no Kafka.

**Cada coluna, por quê:**

| Coluna | Por quê |
|---|---|
| `event_id UNIQUE` | **idempotência**: se a saga decidiu publicar 2× o mesmo evento (redelivery), o conflito é ignorado |
| `topic` / `key` / `payload` | exatamente o que o produtor precisa (tópico, partição por `key`=order_id, corpo JSON) |
| `traceparent` | guarda o **trace W3C** de quem decidiu o evento → o relay **continua a cadeia** no Jaeger (sem perder o span original) |
| `published_at` | marcador de "já publicado" (o relay marca após publicar) |
| `claimed_at` | **claim** para múltiplos relays: `FOR UPDATE SKIP LOCKED` seleciona só o que ninguém reivindicou; claims órfãos (> 60 s) são reclamados de novo |
| `idx_outbox_pending (published_at, id) WHERE published_at IS NULL` | **índice parcial** — o `ClaimPending` consulta só pendentes sem varrer a tabela inteira |
| **purga** | eventos publicados há > 7 dias são deletados (`PurgePublished`) — a outbox não cresce para sempre |

### 2.4 A transação atômica — como as 3 tabelas se encaixam

Desde a **Etapa 7.4**, o orquestrador e os workers gravam **estado + journal + outbox em UMA
transação** (`SagaUnitOfWork` / `internal/infrastructure/uow`):

```
BEGIN;
  INSERT INTO sagas ...            -- (ou UPDATE) estado novo
  INSERT INTO saga_events ...      -- journal (IN ou OUT do evento)
  INSERT INTO outbox ...           -- evento decidido a publicar
COMMIT;   -- se qualquer INSERT falhar → ROLLBACK de tudo
```

**Por que isso importa:** sem a transação única, existiam **janelas residuais** — por exemplo,
a saga salva como `PAYMENT_APPROVED`, mas o `INVENTORY_COMMAND` não vai para a outbox (processo
morreu no meio) → saga presa. A idempotência cobria os buracos, mas a tx única os **elimina**:
ou decidiu E publica, ou não decide nada.

---

## 3. Banco de LEITURA (`saga_read`)

### 3.1 `order_views` — o read model (CQRS)

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

**Por que existe:** é o modelo **desenhado para leitura** — uma linha por pedido com o status
final e um `timeline` (JSONB) com todos os eventos. Quem consome não precisa entender a saga;
pergunta "qual o status do pedido?" e responde em 1 SELECT.

**Cada coluna, por quê:**

| Coluna | Por quê |
|---|---|
| `order_id` (PK) | um pedido = uma linha (upsert pelo projector) |
| `current_status` | o status **final conhecido** na projeção |
| `last_event_type` / `last_event_at` | qual evento atualizou por último e quando |
| `transaction_id` | útil para auditoria/estorno |
| `notification_error` / `payment_refund_failed` | **flags de negócio** do encerramento (notificação falhou mas completou; estorno falhou) — sem precisar "ler nas entrelinhas" da timeline |
| `timeline` (JSONB) | append-only dos eventos (rastreio embutido no read model) |
| **estado terminal é final** | o `projector` **não regride** `COMPLETED`/`FAILED` (evento atrasado de outro tópico só entra no `timeline`) — fix da Etapa 7.4/7.5 |

### 3.2 `processed_events` — dedup do projector

```sql
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Por que existe:** o projector consome do Kafka com **at-least-once** — a mesma mensagem pode
chegar 2×. `event_id` **UNIQUE** garante que o mesmo evento é aplicado no read model **uma
única vez** (o INSERT retorna 0 linhas na 2ª tentativa → `MarkProcessed` devolve `false` e o
evento é ignorado).

---

## 4. Migrations — como o esquema evolui

- **`golang-migrate`** aplica os arquivos `NNNNN_nome.up.sql`/`.down.sql` em ordem
  (`docker-compose` serviço `migrations` para escrita; `migrations-read` para leitura).
- No **K8s**, as migrations são **Jobs** (o ConfigMap carrega os `.sql` e o Job roda o
  `migrate` — escrita e leitura em containers separados).
- **Regra:** nunca editar uma migration já aplicada; criar uma nova `NNNNN_*.sql`.

| Migration | O que faz |
|---|---|
| `000001_create_sagas` | tabela `sagas` |
| `000002_create_saga_events` | journal + índice de correlação |
| `000003_create_outbox` | outbox + índice parcial de pendentes |
| `000004_add_outbox_traceparent` | coluna `traceparent` (rastreio W3C na outbox) |
| `000005_add_outbox_claim` | coluna `claimed_at` (claims/SKIP LOCKED p/ múltiplos relays) |
| `migrations-read/000001_create_order_views` | read model |
| `migrations-read/000002_create_processed_events` | dedup do projector |

---

## 5. Fluxo de uma saga ATRAVÉS das tabelas

Seguindo um pedido `order-001` até `COMPLETED`, olhando as tabelas:

```
sagas:        order-001  PENDING → PAYMENT_PENDING → PAYMENT_APPROVED → INVENTORY_RESERVED → NOTIFIED → COMPLETED
              (retry_count, transaction_id preenchido na aprovação)

saga_events:  1 | IN   ORDER_CREATED        (orchestrator)
              2 | OUT  PAYMENT_COMMAND      (orchestrator)
              3 | IN   PAYMENT_COMMAND      (worker-payment)
              4 | GATEWAY_REQUEST          (worker-payment)
              5 | GATEWAY_RESPONSE         (worker-payment)
              6 | OUT  PAYMENT_RESULT      (worker-payment)
              7 | IN   PAYMENT_RESULT      (orchestrator)
              8 | OUT  INVENTORY_COMMAND   (orchestrator)
              ... (inventory, notification)
              20| OUT  ORDER_COMPLETED     (orchestrator)

outbox:       PAYMENT_COMMAND → publicado ✓
              INVENTORY_COMMAND → publicado ✓
              NOTIFICATION_COMMAND → publicado ✓
              ORDER_COMPLETED → publicado ✓   (todas com published_at != NULL)

order_views:  order-001  COMPLETED  | timeline: [8 eventos] | transaction_id=tx-...
processed_events: (event_ids já aplicados pelo projector — dedup)
```

> **Leitura importante:** `saga_events` é o "traço" completo; `sagas` é o "agora";
> `outbox` é o "a publicar"; `order_views` é o "resumo para consulta".

---

## 6. Consultas de diagnóstico mais úteis

```sql
-- 1) Fila do pipeline (sagas em status intermediários)
SELECT current_status, count(*) FROM sagas GROUP BY 1 ORDER BY 2 DESC;

-- 2) Onde um pedido está / o que aconteceu
SELECT order_id, current_status, retry_count, transaction_id, updated_at FROM sagas WHERE order_id='X';
SELECT component, direction, event_type, status_anterior, status_atual, created_at
  FROM saga_events WHERE order_id='X' ORDER BY id;

-- 3) Outbox: o que ainda não foi publicado / há quanto tempo
SELECT count(*) FROM outbox WHERE published_at IS NULL;
SELECT max(created_at), max(now() - created_at) AS idade FROM outbox WHERE published_at IS NULL;

-- 4) Limpar tabelas para um novo benchmark (estudo)
TRUNCATE sagas, saga_events, outbox RESTART IDENTITY CASCADE;
-- (no banco de leitura) TRUNCATE order_views, processed_events RESTART IDENTITY CASCADE;

-- 5) Read model de um pedido
SELECT * FROM order_views WHERE order_id='X';
```

> ⚠️ **Backup/segurança:** comandos de TRUNCATE/DELETE em produção devem ser feitos com
> extremo cuidado — este é um projeto de estudo, mas a disciplina vale.




