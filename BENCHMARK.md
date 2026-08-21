# Benchmark — Fase 5 (Escalabilidade)

> Resultados medidos no ambiente local (docker-compose, Kafka 1 broker, Postgres single).
> Objetivo: comparar o processamento **sequencial** vs **concorrente (escala horizontal)**.

## Ambiente

- Kafka KRaft 3.9 (1 broker), tópicos com **4 partições**.
- Postgres de escrita/leitura em containers locais.
- load-generator: publicação em lotes (`-batch 500`).

## Método

1. Resetar os bancos (`TRUNCATE sagas, saga_events, outbox, order_views, processed_events`).
2. Publicar **3.000 pedidos** (`ORDER_CREATED`).
3. Aguardar **60 s**.
4. Contar o estado das sagas no banco de escrita.

## Resultados (3.000 pedidos, 60 s)

| Cenário | PAYMENT_PENDING (fila) | PAYMENT_APPROVED | INVENTORY_RESERVED | FAILED | COMPLETED |
|---|---|---|---|---|---|
| **A: 1 réplica** (1 orquestrador, 1 worker/etapa, 1 relay) | 2.012 | 839 | 0 | 149 | 0 |
| **B: 3 réplicas** (orquestrador + workers) + **2 relays** | **164** | 1.897 | 16 | 345 | 0 |

### Leitura

- **Ingestão** (lote): ~497 eventos/s (limite do load-generator).
- **Processamento**: o cenário B drenou a fila de `PAYMENT_PENDING` de 2.012 para **164
  (~12× menos pendentes)** em 60 s → o **throughput de processamento escala com réplicas**.
- As sagas completas exigem o ciclo inteiro do pipeline (várias ondas); em 60 s ambas as
  configurações ainda estavam processando, mas B avançou muito mais etapas.

## Autoscaler (lag → réplicas)

Teste com 10.000 pedidos e `AUTOSCALE_HIGH_LAG=200`:

```
11:39:31 action=scale svc=orchestrator replicas=2   (lag=3582)
11:39:39 action=scale svc=orchestrator replicas=3   (lag=1602)
11:39:43 lag=0 replicas=3                            (backlog drenado)
11:40:07 action=scale svc=orchestrator replicas=2   (lag=0 por 20s → reduz)
```

- O autoscaler subiu 1 → 2 → 3 réplicas conforme o lag, drenou o backlog e reduziu
  quando ocioso (mesma política do KEDA/HPA).

## Gargalos identificados e corrigidos durante a Fase 5

| Gargalo | Correção |
|---|---|
| `outbox-relay` publicava 1 mensagem/round-trip (~1 evento/s) | **`PublishBatch`** (lote) → centenas de eventos/s |
| 1 partição por tópico (sem paralelismo) | **4 partições** + consumer groups |
| 2 relays processando a mesma outbox | **`FOR UPDATE SKIP LOCKED`** (claims) |
| Consumers single-threaded | **`SAGA_WORKERS`** (Readers concorrentes no mesmo grupo) |

## Como reproduzir

```bash
make up
# Cenário A (padrão, 1 réplica)
KAFKA_BROKERS=localhost:9094 go run ./cmd/load-generator -count 3000 -batch 500 -prefix bA
sleep 60
docker-compose exec postgres psql -U saga -d saga -c "SELECT current_status, count(*) FROM sagas WHERE order_id LIKE 'bA%' GROUP BY 1 ORDER BY 2 DESC;"

# Cenário B (escalado)
docker-compose up -d --no-deps --scale orchestrator=3 --scale worker-payment=3 --scale worker-inventory=3 --scale worker-notification=3 --scale outbox-relay=2 orchestrator worker-payment worker-inventory worker-notification outbox-relay
# (resetar os bancos antes) ... rodar o mesmo procedimento com prefixo bB

# Autoscaler (roda no host)
make autoscale   # configurações via AUTOSCALE_* env
```
