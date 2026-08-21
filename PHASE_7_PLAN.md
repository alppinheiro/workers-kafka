# Fase 7: Performance e Prontidão de Produção (Outbox-Relay, Rastreabilidade e Escala)

> **Objetivo:** eliminar o gargalo de throughput do pipeline (saga distribuída + eventos) e garantir
> que o fluxo "flua" com rastreabilidade completa — respondendo ao requisito: *"se não tem
> performance, não adianta ser saga distribuída por eventos"*.

## 1. Diagnóstico (dados de 21/08/2026)

### 1.1 O que o benchmark revelou

| Cenário | Config | Throughput processado | Sagas COMPLETED em ~2 min |
|---|---|---|---|
| A6 | 1 réplica, `SAGA_WORKERS=1`, 1 relay | ~48 ev/s | 0 |
| B3 | 1 réplica, `SAGA_WORKERS=3`, 1 relay | ~47 ev/s | 0 |
| C3 | 3 réplicas, `SAGA_WORKERS=1`, 2 relays | ~21 ev/s¹ | 174 |

¹ distribuído entre réplicas.

- **Todas as configurações ficam limitadas a ~48 ev/s** porque o **outbox-relay** é o gargalo único.
- `SAGA_WORKERS` (goroutines) e réplicas de consumers **não escalam** enquanto o relay não acompanhar.
- Referência: a mesma stack, antes do restart, processou **~360 ev/s** (2.322 COMPLETED em 60 s) —
  provando que o código dos consumers **suporta ~360 ev/s**; o teto caiu para ~48 ev/s após
  `docker-compose down -v && make up` (ver §1.3).

### 1.2 Análise do gargalo (outbox-relay)

O ciclo do relay (`cmd/outbox-relay/main.go`):

```
for { select { case <-time.After(1s): relayOnce() } }   ← timer FIXO de 1s → teto 100/s
relayOnce: ClaimPending(100) → CountPending → PublishBatch(100) → 100× MarkPublished + 100× log.Printf
```

Medições empíricas (Postgres e Kafka reais do docker-compose):

| Operação | Custo medido | Verdict |
|---|---|---|
| `MarkPublished` ×100 (UPDATEs individuais) | **1 ms** | ✅ não é o gargalo |
| `MarkPublished` em lote (`id = ANY($1)`) | **2 ms** | ✅ |
| Kafka `PublishBatch` (100 msgs, acks=1) | broker sustenta **43.859 rec/s** | ✅ não é o gargalo |
| `ClaimPending` (usa `idx_outbox_pending`) | **0,03 ms** | ✅ índice parcial já existe |
| `time.After(1s)` + batch fixo 100 | teto estrutural de **100/s** | ❌ **gargalo** |
| 100× `log.Printf` por ciclo (stdout → docker) | ~1 s/ciclo observado | ❌ **gargalo** (50/s efetivos) |

**Causa raiz:** o ciclo é *serial e bloqueado*: 1 lote por segundo + 100 logs formatados/gravados
por ciclo. Com o processamento real de ~1 s por ciclo (logs), o throughput cai para **50/s**.

## 2. Validação de arquitetura — "o mercado realmente usa saga assim?"

**Sim.** O projeto implementa exatamente os padrões canônicos de microsserviços
(microservices.io — Chris Richardson):

| Padrão | Como o projeto implementa | Uso no mercado |
|---|---|---|
| **Saga orquestrada** | orquestrador central decide cada etapa e compensa (PAYMENT_REFUND etc.), coordenação via eventos | Temporal, AWS Step Functions, Camunda/Zeebe, Netflix Conductor |
| **Transactional Outbox** | estado + outbox na mesma transação (por serviço); `outbox-relay` publica no Kafka | Spring Transactional Outbox, Debezium (CDC), Eventuate |
| **Message Relay (polling)** | `ClaimPending` com `FOR UPDATE SKIP LOCKED` | polling publisher (escolha determinística e simples) |
| **Ordem/particionamento** | chave `order_id` → mesma partição em todos os tópicos | padrão de correlação de saga em Kafka |
| **Idempotência** | UNIQUE em `event_id` / `processed_events` | exigência de produção (at-least-once) |
| **Rastreabilidade** | W3C `traceparent` + `saga_events` (journal) + `saga_id`/`order_id` em logs | OpenTelemetry + event sourcing/journal |

**Onde o mercado costuma ir além (nossos próximos passos):**
1. Relay **sem polling no banco**: CDC (Debezium) lê o WAL → não compete com o banco e escala
   melhor. Alternativa imediata (plano abaixo): otimizar o polling (batch + sem log + contínuo).
2. **Orquestrador mais durável**: Temporal/Step Functions mantêm o workflow em engine dedicada.
   Nosso journal (`saga_events`) + reload no boot já dão durabilidade equivalente para o caso.
3. **Isolamento de saga (countermeasures)**: o projeto já roda sagas por `order_id` em partição

## 3. Plano de otimização (etapas incrementais e validadas)

### Etapa 7.1 — Outbox-relay sem gargalo (impacto: ~10×)

Alterações em `cmd/outbox-relay/main.go` + `outbox_repository.go`:

1. **Remover o log por evento** (causa o custo de ~1 s/ciclo): 1 log agregado por ciclo
   (`outbox-relay: publicados=100 total_published=N duracao=1.2s`).
2. **Batch dinâmico**: `batchSize` configurável (env `OUTBOX_BATCH_SIZE`, default 500).
3. **Loop contínuo com backoff**: se o lote retornou cheio, processa imediatamente o próximo
   (sem esperar 1 s); quando vazio, `sleep` curto (250 ms). Elimina o teto de 1 ciclo/s.
4. **`MarkPublished` em lote**: `UPDATE outbox SET published_at=now() WHERE id = ANY($1)` —
   1 round-trip; robusto quando o Postgres for remoto (RDS).
5. **`CountPending` opcional/esparso** (a cada N ciclos) e **purga/retention**:
   `DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - interval '7 days'`
   (job agendado ou no próprio relay a cada hora).

**Validação:** `make integration` verde + benchmark de throughput (método do `BENCHMARK.md`):
esperado ≥ **300 ev/s** com 1 relay.

**✅ Resultado (21/08/2026):** relay passa a **~485 ev/s** (lote 500/ciclo ~1s) vs ~50 ev/s antes —
**~9,7×**. No fluxo completo o relay deixou de ser o gargalo; o novo limitador é o consumo
(orquestrador em bursts intermitentes) → **Etapa 7.2**.

### Etapa 7.2 — Consumers: batch de commit (impacto: 2–4× acima de 400 ev/s)

- `ReaderConfig` com `CommitInterval` + commit em lote após N mensagens (ex.: 50), mantendo
  ordem por partição e idempotência (reprocessamento de até 50 é inofensivo).
- `SAGA_WORKERS` continua sendo o paralelismo intra-instância; o commit em lote reduz os
  round-trips ao broker.

**Validação:** benchmark A/B (`SAGA_WORKERS=1` vs `3`) agora COM relay otimizado.

### Etapa 7.3 — Rastreabilidade completa (produção)

- **Índice de correlação**: `saga_events(order_id, created_at)` + `event_id` — consulta de
  auditoria por pedido sem scan.
- **Métricas por etapa**: já existe `saga_events_processed_total{service,event_type}`; adicionar
  **lag por consumer group** e **idade da outbox** no dashboard (rascunho no grafana).
- **DLQ alerting**: alerta quando `saga_events_dlq_total` cresce (Grafana Alerting / Prometheus
  `rules.yml`).
- **Journal como fonte de verdade**: documentar o fluxo de auditoria (`saga_events`) como o
  "trace de negócio" (complementar ao trace técnico no Jaeger).

### Etapa 7.4 — Pendência estrutural (transação atômica)

- Unificar estado + journal + outbox em **uma única transação** por handler (orquestrador e
  workers). Elimina as janelas residuais (hoje cobertas por idempotência).

## 4. Ordem de execução e critérios de pronto

| Etapa | Risco | Dependência | DoD |
|---|---|---|---|
| 7.1 relay | baixo (código isolado) | — | `make integration` + ≥300 ev/s com 1 relay |
| 7.2 commit batch | médio (ordem/idempotência) | 7.1 | benchmark A/B verde; DLQ zero no teste |
| 7.3 rastreabilidade | baixo | 7.1 | consulta de auditoria < 50 ms; alerta DLQ funcional |
| 7.4 transação atômica | alto (regressão) | 7.2 | `make check` + Testcontainers verdes |

**Recomendação:** executar 7.1 primeiro (maior impacto, menor risco) e re-medir com o método do
`BENCHMARK.md` — o mesmo método que revelou o gargalo, agora para provar a correção.

   única, evitando as anomalias de concorrência do padrão.

**Conclusão:** o design é correto e é o que o mercado usa. O problema é **execução/performance**
do relay — resolvível sem mudar a arquitetura.


### 1.3 Outros achados

- **Outbox cresce para sempre**: a tabela tinha **19.384 linhas** acumuladas (só `published_at`
  é marcado; nada é removido). Em produção isso degrada consultas e o `CountPending`.
- **`CountPending` a cada ciclo**: `SELECT count(*)` na tabela inteira a cada ciclo — desnecessário
  com histórico grande.
- **Consumer commita 1 mensagem por vez** (`CommitMessages` por evento): suficiente para ~400 ev/s
  (provado), mas limita acima disso — próximo gargalo após otimizar o relay.
- **Pendência conhecida**: estado + journal + outbox em 3 operações (não atômicas). Coberta por
  idempotência (`event_id`), mas a transação única elimina as janelas residuais.
