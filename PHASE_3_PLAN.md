# Fase 3: Resiliência — Outbox Pattern, Idempotência e DLQ

> Plano de execução da Fase 3. Consultar junto com `PHASE_2_PLAN.md`, `instructions.md` e `EVOLUTION_PLAN.md`.

## Objetivo

Eliminar os pontos fracos de resiliência deixados pela Fase 2:

1. **Idempotência**: redelivery de mensagens (at-least-once) não pode causar loops ou efeitos duplicados.
2. **DLQ**: mensagens definitivamente inválidas (poison pills, eventos fora de ordem genuínos) vão para tópicos de erro em vez de travar o fluxo.
3. **Outbox Pattern**: a publicação de eventos deixa de depender de um publish direto no meio do processamento; o evento é registrado no banco e um relé garante a publicação no Kafka.

## Decisões fechadas

| # | Decisão |
|---|---|
| 1 | **Idempotência via journal**: `EventLogRepository.Has(event_id, component)` — o orquestrador e os workers ignoram eventos cujo `IN` já foi registrado no `saga_events`. |
| 2 | **DLQ por tópico**: tópicos `orders.<etapa>.dlq`; erros definitivos (`ErrNonRetryable`) vão para a DLQ e a mensagem é commitada; erros transitórios mantêm o retry atual. |
| 3 | **Outbox no banco de escrita**: tabela `outbox` (event_id único) + `OutboxPublisher` (implementa `application.EventPublisher`) + serviço `outbox-relay` que publica e marca `published_at`. |
| 4 | O `create-order` continua publicando direto no Kafka (não tem banco de escrita). |
| 5 | A atomicidade total (estado + journal + outbox na mesma transação) é documentada como refinamento futuro; nesta fase a ordem é: Save/Append → Outbox, e a reentrega + idempotência cobrem os gaps. |

## Modelo de dados

### `migrations/000003_create_outbox.up.sql`

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     VARCHAR(255) NOT NULL UNIQUE,
    topic        VARCHAR(100) NOT NULL,
    key          VARCHAR(255) NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(published_at, id) WHERE published_at IS NULL;
```

### Tópicos DLQ (via `kafka-init`)

- `orders.created.dlq`, `orders.payment.dlq`, `orders.inventory.dlq`, `orders.notification.dlq`, `orders.status.dlq`

## Estrutura de código

Novos arquivos:

```
internal/infrastructure/persistence/postgres/outbox_repository.go  # Append, FetchPending, MarkPublished
internal/infrastructure/outbox/publisher.go                        # OutboxPublisher (EventPublisher)
cmd/outbox-relay/main.go                                           # relé: outbox -> Kafka
internal/application/errors.go                                     # ErrNonRetryable (sentinel)
migrations/000003_create_outbox.{up,down}.sql
```

Editados:

```
internal/infrastructure/kafka/consumer.go   # DLQ: se handler retorna ErrNonRetryable -> DLQ + commit
internal/infrastructure/kafka/topics.go     # mapeamento de tópicos DLQ
internal/application/ports.go               # + EventLogRepository.Has
internal/application/orchestrator/orchestrator.go  # idempotência no HandleResult/StartOrder
internal/application/worker/*.go            # idempotência nos workers
docker-compose.yml / Makefile / .vscode      # + outbox-relay
cmd/orchestrator + cmd/worker-*             # wiring com OutboxPublisher
```

## Ordem de execução

1. Plano + `EVOLUTION_PLAN.md` (Fase 3 em execução).
2. **Idempotência**: `EventLogRepository.Has` + verificação no orquestrador e workers + testes.
3. **DLQ**: `ErrNonRetryable` + marcar erros definitivos + política no consumer + tópicos + testes.
4. **Outbox**: migration + repositório + publisher + relay + wiring + testes.
5. **Validação e2e** (stack de pé): migrations novas, fluxo feliz, **demonstração de idempotência** (redelivery manual) e **DLQ** (mensagem inválida).
6. Docs: `instructions.md`, `README.md`.

## Fora de escopo nesta fase

- Transação atômica única (estado + journal + outbox) — refinamento futuro documentado.
- API REST.
- Observabilidade formal.
- Concorrência/goroutines (Fase 5).
