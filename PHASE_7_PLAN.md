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

### Etapa 7.2 — Consumers: commit em lote + resiliência (impacto: 195 → 300+ ev/s)

**Contexto (A12):** com 4 partições + relay otimizado, o orquestrador atingiu **195 ev/s** e é o
novo gargalo — limitado pelo **commit por mensagem** (1 round-trip `CommitMessages` por evento).
O relay já publica 235 ev/s.

**Alterações em `internal/infrastructure/kafka/consumer.go`:**

1. **Commit em lote** (mantém ordem por partição + idempotência):
   - Acumular offsets por partição durante o processamento.
   - A cada `N` mensagens (env `KAFKA_COMMIT_BATCH`, default 50) ou a cada `200 ms`, chamar
     `reader.CommitOffsets` com os offsets acumulados.
   - No shutdown/erro, commitar o pendente.
   - Semântica: at-least-once; reprocessar até `N` eventos é inofensivo (idempotência por `event_id`).

2. **Resiliência a erros de coordenação do Kafka** (não derrubar o serviço):
   - Adicionar `UnknownTopicOrPartition`, `LeaderNotAvailable`, `BrokerNotAvailable` etc. ao
     `shouldRetryFetch` — hoje o serviço morre (`exit 1`) se um tópico é recriado.
   - Retry com backoff (2s → 30s) em vez de encerrar.

3. **Configuração**: `KAFKA_COMMIT_BATCH` (default 50), `KAFKA_COMMIT_INTERVAL` (default 200ms).

**Validação:** benchmark A (orquestrador ≥ 300 ev/s); `make check` + Testcontainers verdes;
teste do acumulador de offsets.

**✅ Resultado (21/08/2026):** orquestrador 195 → **216 ev/s** (commit em lote; o ganho é
limitado pela persistência por evento no Postgres). Resiliência: consumer **não morre mais**
com tópico recriado e o **watchdog anti-stall reconecta o reader sozinho** em até 45 s
(validado com os 6 serviços travando).

### Etapa 7.3 — Rastreabilidade completa (produção)

- **Índice de correlação**: `saga_events(order_id, created_at)` + `saga_events(event_id)` —
  consulta de auditoria por pedido sem full scan (hoje não há índice em `order_id`).
- **Métricas**: `saga_consumer_lag` (lag por grupo via admin do Kafka, exposto pelo
  metrics-exporter), `saga_outbox_max_age_seconds`.
- **Alerta DLQ**: `prometheus/rules.yml` — `increase(saga_events_dlq_total[5m]) > 0` →
  alerta `DLQGrowth` (Grafana Alerting).
- **Correção do gauge stale do metrics-exporter**: zerar labels de status que não existem mais
  (`ordersPending.Reset()` antes de `Set`) para o painel refletir o estado real.
- **Journal = trace de negócio**: documentar o fluxo de auditoria (`saga_events`) no README
  (complemento ao trace técnico do Jaeger).

**Validação:** consulta de auditoria < 50 ms; alerta DLQ funcional (teste de disparo).

**✅ Resultado (21/08/2026):**
- **Correção do Grafana**: o dashboard "Saga - Visão Geral" existia, mas os painéis usavam
  `datasource.uid="Prometheus"` enquanto o provisionamento não fixava o UID (gerava aleatório) →
  gráficos vazios. Corrigido com `uid: Prometheus` no `datasource.yml`; dashboard validado via API
  (10 painéis, datasource health OK, queries retornam dados).
- **Gauge stale corrigido**: `ResetOrdersPending()` antes de cada coleta (status que somem não
  mantêm valor antigo).
- **Novas métricas**: `saga_consumer_lag{group,topic}` (lag real por consumer group via admin do
  Kafka — validado: lag=294 sob carga) e `saga_outbox_max_age_seconds`.
- **Alertas**: `SagaDLQGrowth` (DLQ crescente) e `SagaConsumerStalled` (sem progresso com lag)
  no `prometheus/rules.yml`, carregados (estado inactive).

### Etapa 7.4 — Transação atômica única (consistência)

- Unificar `Save` (estado) + `Append` (journal) + `Append` (outbox) em **uma transação** por
  handler (orquestrador e workers) — elimina as janelas residuais cobertas hoje por idempotência.
- `pgx.Tx` + repositórios transacionais (`SagaTxRepository`, `EventLogTxRepository`,
  `OutboxTxRepository`).
- **Validação:** testes de consistência (nenhum evento sem estado; outbox sempre acompanhada);
  Testcontainers verdes.

**✅ Resultado (21/08/2026):**
- **Porta `application.SagaUnitOfWork`**: `WithTx(ctx, fn(tx application.SagaTx) error)` — o
  handler executa todo o processamento do evento em um bloco; commit se `fn` retornar `nil`,
  rollback total em qualquer erro.
- **Implementação `internal/infrastructure/uow`**: `PostgresUnitOfWork` abre `pgx.Tx` e expõe
  repositórios transacionais (`NewSagaRepositoryTx`, `NewEventLogRepositoryTx`,
  `NewOutboxRepositoryTx`) + o publisher da outbox ligado à transação.
- **Repositórios com `DBTX`**: contrato mínimo (Exec/Query/QueryRow) compartilhado por
  `*pgxpool.Pool` e `pgx.Tx` — os mesmos métodos servem fora e dentro de transação.
- **Orquestrador e workers atômicos**: `StartOrder`/`HandleResult` e os 3 workers passam a
  gravar estado + journal + outbox na mesma transação por evento (antes eram 3 operações
  independentes cobertas só por idempotência).
- **Testes de consistência**: `TestUnitOfWork_AtomicCommit` (sagas + saga_events + outbox
  persistem juntas) e `TestUnitOfWork_AtomicRollback` (erro no bloco → nada é persistido) com
  Postgres real (DATABASE_URL) e versão Testcontainers (`TestUnitOfWorkRollbackWithContainer`);
  fluxo completo do container também valida outbox populada. `make check` verde.

### Etapa 7.5 — Validação final de produção (DoD)

- Benchmark completo A/B/C com tudo otimizado (registrar no `BENCHMARK.md`).
- **Testes de resiliência**: restart de cada serviço no meio do fluxo (a saga continua de onde
  parou); tópico recriado (o consumer não morre); relay duplicado (SKIP LOCKED sem duplicar);
  worker caído (compensação correta).
- `make check` + `make integration` verdes.
- Runbook operacional no README (subir stack, monitorar, recuperar DLQ).

## 4. Ordem de execução e critérios de pronto

| Etapa | Risco | Dependência | DoD |
|---|---|---|---|
| 7.1 relay | baixo | — | ✅ **feito** — relay ~485 ev/s |
| **7.2 commit em lote + resiliência** | médio | 7.1 | orquestrador ≥ 300 ev/s; sobrevive a tópico recriado |
| 7.3 rastreabilidade | baixo | 7.1 | auditoria < 50 ms; alerta DLQ; gauge correto |
| 7.4 transação atômica | alto | 7.2 | consistência; Testcontainers verdes |
| 7.5 validação produção | médio | 7.2–7.4 | benchmark A/B/C + resiliência + runbook |

**Recomendação:** implementar **7.2** agora (maior ganho de performance restante + resiliência),
depois **7.3** (rastreabilidade — requisito de produção), **7.4** (consistência) e **7.5** (validação).

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
