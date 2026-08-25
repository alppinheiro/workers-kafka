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

## Benchmark de Throughput — pós Fase 5/6 (medido com Prometheus)

> Preenche a lacuna do benchmark anterior (que media só *backlog residual*): medimos agora
> **eventos/s reais de processamento por serviço** via `rate()` dos counters Prometheus
> (`saga_events_processed_total`), com fonte de verdade no Postgres.

### Método

1. Stack limpa (`make up`), tópicos com 4 partições, consumer groups resetados (`--to-latest`).
2. `TRUNCATE` dos bancos e publicação de **3.000 pedidos** (`load-generator -count 3000 -batch 500`).
3. Medição: `rate(saga_events_processed_total[<janela>])` no Prometheus + contagem por status no Postgres.

### Resultados — janela de ~128 s

| Cenário | Config | Orquestrador | worker-payment | outbox-relay (pub.) | Sagas 2ª+ onda | COMPLETED |
|---|---|---|---|---|---|---|
| **A6** | 1 réplica, `SAGA_WORKERS=1`, 1 relay | 48,6 ev/s | 46,2 ev/s | 49,2 ev/s | 0 | 0 |
| **B3** | 1 réplica, `SAGA_WORKERS=3`, 1 relay | 47,8 ev/s | 46,0 ev/s | 49,0 ev/s | 0 | 0 |
| **C3** | 3 réplicas, `SAGA_WORKERS=1`, **2 relays** | 20,3 ev/s¹ | 10,9 ev/s¹ | 40,6 ev/s² | **1.186** | **174** |

¹ Distribuído entre 3 réplicas (Prometheus alterna o target → rate por serviço diluído).
² 2 relays no mesmo target DNS → medição subestimada (fonte: Postgres).

### Drenagem completa (3.000 pedidos → 100% COMPLETED+FAILED)

| Cenário | Config | Tempo total | COMPLETED | FAILED |
|---|---|---|---|---|
| **A7** | 1 réplica, `SAGA_WORKERS=1`, 1 relay | **400 s** | 2.341 | 659 |
| **C3** | 3 réplicas, `SAGA_WORKERS=1`, 2 relays | **~340 s** | 2.305 | 695 |

### Leitura (o que o teste de goroutines revelou)

- **`SAGA_WORKERS` (goroutines) NÃO muda o throughput no ambiente local**: A6 ≈ B3 (48 vs 47 ev/s).
  Motivo: o **outbox-relay é o gargalo** — ~49 ev/s = 1 lote de 100 a cada ~2 s, com `MarkPublished`
  sequencial (1 `UPDATE` por evento) — e ele **não escala com goroutines**.
- **Escala horizontal (réplicas + 2 relays) adianta o pipeline em ondas**: em 2 min o C3 já tinha
  174 sagas COMPLETED (A6/B3: 0) e a drenagem total caiu ~15% (400 s → ~340 s).
- O ganho é limitado porque o **Postgres single-instance** concentra os `MarkPublished` + atualizações
  das sagas — com 2 relays o gargalo desloca para o banco, não para os consumers.

### ⚠️ Observação — degradação após restart da stack

- A stack que rodava há ~35 min (Fase 6) processou **~330–360 ev/s** (3.000 pedidos, 2.322 COMPLETED
  em 60 s). Após `docker-compose down -v && make up`, a **mesma stack** caiu para **~48 ev/s** (~7×).
  Hipóteses a investigar: cadência do `outbox-relay` (lote de 100 a cada ~2 s), latência do Kafka
  1-broker pós-reinício e concorrência no Colima (2 CPUs / 4 GB).

## Benchmark — Etapa 7.1 aplicada (outbox-relay otimizado)

| Métrica | Antes (7.0) | Depois (7.1) | Ganho |
|---|---|---|---|
| Throughput do relay (outbox → Kafka) | **~50 ev/s** (lote 100 / ciclo 2 s / log por evento) | **~485 ev/s** (lote 500 / ciclo ~1 s / log agregado) | **~9,7×** |
| Drenagem de 5.000 eventos na outbox | ~100 s (estimado) | **~10 s** (medido) | ~10× |

**Como medido:** 5.000 linhas inseridas diretamente na outbox → relay publica em lotes de 500
(`ciclo publicados=500 duracao=1.03s` → 485 ev/s) → outbox zerada em ~10 s.

**Resultado no fluxo completo (A11):** o relay passou a ser limitado pela **entrada** (orquestrador
gera ~60 eventos/s), não mais por ele mesmo. O novo gargalo é o consumo (orquestrador lendo em
bursts intermitentes) → **Etapa 7.2 (commit em lote) e investigar `kafka-go GroupTopics`**.

> ⚠️ O A11 foi medido com **1 partição por tópico** (regressão do laboratório: o auto-create do
> broker criou os tópicos com 1 partição ao recriá-los). Invalida as taxas desse cenário.

## Benchmark — A12 (7.1 + 4 partições corretas)

| Métrica | A6 (7.0, relay antigo) | A12 (7.1 + 4 partições) |
|---|---|---|
| Orquestrador | 48,6 ev/s | **195,3 ev/s** |
| outbox-relay (pub.) | 49,2 ev/s | **235,1 ev/s** |
| Sagas COMPLETED em ~69 s | 0 | **2.291** (+709 FAILED = 100%) |

- **O fluxo completo drena 3.000 pedidos em ~69 s** com o relay otimizado e 4 partições.
- Novo gargalo: **consumo** (orquestrador 195 ev/s vs relay 235 ev/s) → commit por mensagem
  (~1 round-trip/evento) é o limite da Etapa 7.2.

## Benchmark — A13/A17 (Etapa 7.2: commit em lote + watchdog anti-stall)

| Métrica | A12 (7.1) | A13 (7.2) | A17 (7.2, fluxo saudável) |
|---|---|---|---|
| Orquestrador | 195,3 ev/s | **216,1 ev/s** | — |
| outbox-relay (pub.) | 235,1 ev/s | 248,1 ev/s | — |
| 3.000 sagas em ~69 s | 2.291 COMPLETED | 2.302 COMPLETED | — |
| 2.000 sagas em ~60 s | — | — | **1.505 COMPLETED + 495 FAILED (100%)** |

**Resiliência (7.2):**
- Orquestrador **sobrevive** à recriação de tópico (`UnknownTopicOrPartition` agora é retry,
  não `exit 1`).
- **Watchdog anti-stall**: quando o kafka-go para de fazer fetch (travamento conhecido sem erro
  aparente — reproduzido com `--alter --partitions`), o consumer detecta em até 45 s e **reconecta
  o reader sozinho** (`phase=stall-detected` → `phase=reconnect`), sem intervenção. Validado com
  todos os 6 serviços travando ao mesmo tempo.
- `restart: unless-stopped` nos serviços do pipeline (camada extra de self-healing).




## Benchmark — Etapa 7.5 (7.1–7.4 aplicados — validação final de produção)

> Medido em 21/08/2026 com todas as otimizações (relay ~485 ev/s, commit em lote,
> watchdog anti-stall, transação atômica). Fonte de verdade: estado das sagas no Postgres.

### Método

1. Reset de bancos (`TRUNCATE sagas, saga_events, outbox, order_views, processed_events`).
2. Reset de consumer groups (`--to-latest`).
3. Publicar **3.000 pedidos** (`load-generator -count 3000 -batch 500`).
4. Janela de **80 s**; contar o estado das sagas + outbox pendente.

### Resultados (3.000 pedidos, 80 s)

| Cenário | Config | COMPLETED | FAILED | Total | Outbox pendente | rate[60s] |
|---|---|---|---|---|---|---|
| **A** | 1 réplica, `SAGA_WORKERS=1`, 1 relay | **2.339** | 661 | 3.000 | **0** | 258 ev/s |
| **B** | 1 réplica, `SAGA_WORKERS=3`, 1 relay | 2.257 | 743 | 3.000 | **0** | 261 ev/s |
| **C** | 3 réplicas, `SAGA_WORKERS=1`, **2 relays** | 2.305 | 695 | 3.000 | **0** | —¹ |

¹ Janela pós-drenagem (a drenagem do C terminou antes da janela de medição → rate 0).

### Leitura

- **As 3 configurações drenam 3.000 pedidos em ~80 s** (COMPLETED + FAILED = 3.000 e
  **outbox = 0** em todas). O pipeline **deixou de ser o gargalo nesse volume**: no
  pré-7.1 (A6), o mesmo fluxo deixava 2.012 pedidos presos na fila e 0 concluídos.
- `SAGA_WORKERS` intra-instância **não muda throughput** (A ≈ B): os round-trips de
  Postgres/Kafka dominam (confirmando a Fase 5).
- Escala horizontal (C) só mostra vantagem em volumes **acima de 3.000** — para
  demonstrá-la, subir o volume e/ou a taxa de ingestão.

### Resiliência — validada (Etapa 7.5)

| # | Teste | Procedimento | Resultado |
|---|---|---|---|
| **R1** | Restart do orquestrador no meio do fluxo | `docker-compose restart orchestrator` após publicar 1.000 pedidos | **1.000/1.000 sagas drenadas** (775 COMPLETED + 225 FAILED); outbox = 0 — nenhuma saga perdida |
| **R2** | Tópico deletado no meio do fluxo | deletar `orders.payment` durante o processamento (auto-recreate pelo broker) | **Consumers não morrem** (`UnknownTopicOrPartition` = retry; watchdog reconecta). Perda inerente da deleção: 64 sagas em compensação aguardam mensagens perdidas |
| **R3** | Relay duplicado (2 relays) | `--scale outbox-relay=2` + carga de 1.500 pedidos | **1.500/1.500 drenadas**; **0 duplicatas na outbox** e **0 event_id duplicado no tópico** (`FOR UPDATE SKIP LOCKED`) |
| **R4** | Worker-payment caído | `docker-compose stop worker-payment` por 20 s no meio do fluxo | Sagas ficam em espera sem corromper; **800/800 drenadas** após religar |

> Nota sobre o R2: a deleção de tópico é uma ação destrutiva de administrador — a
> perda das mensagens em voo é inerente ao Kafka. O teste prova que **o processo
> sobrevive**; para alterar partições sem perda, usar `--alter` (o watchdog reconecta
> o reader no stall conhecido do kafka-go).



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

## Teste de carga sustentada (stress, 120k pedidos)

Para carga máxima com monitoramento contínuo, use `scripts/stress.sh` e o runbook
`docs/STRESS_TEST.md` (matriz de diagnóstico: sintoma → causa → como resolver).

**Resultado de referência** (120.000 pedidos, Kafka 1 broker local):

- Ingestão: **~496 ev/s** (teto do produtor local de 1 broker).
- Processamento: pico de **~2.350 ev/s** (o pipeline processa mais rápido do que ingere).
- **Gargalo sob carga**: `outbox-relay` a ~500 ev/s (outbox acumulou ~117k pendentes);
  com **2 réplicas** a drenagem acelera ~2× (zero duplicação via `SKIP LOCKED`).
- `FAILED` ~7% = recusa real de negócio (taxa de pagamento 0.85), não bug.
- Para 2.000 ev/s sustentados de entrada: **≥4 réplicas de relay** (ou `OUTBOX_BATCH_SIZE`
  maior) + Kafka com mais brokers/partições (EKS).

