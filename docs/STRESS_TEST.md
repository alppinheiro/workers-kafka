# Teste de Carga Sustentada & Runbook de Diagnóstico

> Documenta o teste de stress do pipeline sob carga máxima no ambiente local
> (docker-compose, Kafka 1 broker, Postgres single) e, principalmente, **como
> identificar, rastrear e resolver** cada problema que aparece sob pressão.

---

## 1. Como rodar o teste

```bash
make up                       # sobe a stack (imagens já existentes)
scripts/stress.sh 120000      # publica 120.000 pedidos + monitora a cada ~15s
```

O `scripts/stress.sh`:
1. Reseta bancos (`TRUNCATE sagas, saga_events, outbox, order_views, processed_events`).
2. Reseta consumer groups (`--reset-offsets --to-latest`).
3. Publica N pedidos via `load-generator` (em background) — mede a ingestão real.
4. **FASE 1**: amostra a cada 15s durante a publicação (sagas, outbox, lag, throughput).
5. **FASE 2**: amostra a cada 20s por até 6 min após o fim da publicação (drenagem).

---

## 2. Métricas e onde ver cada uma

| O que | Onde consultar | Como |
|---|---|---|
| Ingestão (produtor) | `load-generator` | último log `load-generator finalizado` (`eventos_por_segundo`) |
| Processamento (consumers) | Prometheus | `sum(rate(saga_events_processed_total[60s]))` |
| **Outbox pendente** (a fila real) | Postgres | `SELECT count(*) FROM outbox WHERE published_at IS NULL` |
| Lag por consumer group | Kafka CLI | `kafka-consumer-groups.sh --all-groups --describe` (dentro do container `kafka`) |
| Sagas por estado | Postgres | `SELECT current_status, count(*) FROM sagas GROUP BY 1` |
| Memória/CPU | `docker stats` | por container |
| Erros/retries | Logs JSON | `docker-compose logs <serviço> \| grep '"level":"WARN\\|ERROR"'` |

> **A fila real do sistema é a outbox**, não o Kafka: o Kafka absorve tudo (lag baixo);
> o gargalo de ingestão no Kafka é o `outbox-relay`.

---

## 3. Matriz de diagnóstico (sintoma → causa → como resolver)

### 3.1 Outbox pendente crescendo / drenando devagar
- **Sintoma**: `outbox WHERE published_at IS NULL` cresce ou não cai.
- **Causa**: `outbox-relay` é o gargalo (~500 ev/s com 1 réplica, ciclo claim→publish→mark).
  Cada pedido gera ~8 eventos na outbox; a produção de eventos (~2.200 ev/s) supera a
  capacidade do relay.
- **Como confirmar**: logs do relay (`ciclo do relay concluído publicados=500 duracao=~1s`)
  → está no teto; `docker-compose logs outbox-relay | tail`.
- **Como resolver** (do mais simples ao ideal):
  1. **Escalar o relay**: `docker-compose scale outbox-relay=2` (o `FOR UPDATE SKIP LOCKED`
     garante zero duplicação com múltiplas instâncias — validado na Fase 7.4).
  2. Aumentar `OUTBOX_BATCH_SIZE` (ex.: 2000) no compose/Helm.
  3. No K8s: Deployment com réplicas (KEDA não escala relay por lag — usar HPA/replicas fixas).

### 3.2 Sagas presas em `PAYMENT_PENDING` (ou outro intermediário)
- **Sintoma**: muitas sagas no mesmo status intermediário e não avançam.
- **Causa**: é o reflexo do §3.1 — o comando da próxima etapa está na outbox esperando o
  relay publicar.
- **Como confirmar**: `SELECT count(*) FROM outbox WHERE published_at IS NULL` alto; lag do
  tópico da etapa baixo (o Kafka ainda nem recebeu o comando).
- **Como resolver**: ver §3.1 (acelerar a drenagem da outbox). Não é bug de lógica.

### 3.3 `FAILED` crescendo durante o stress
- **Sintoma**: sagas `FAILED` aumentam.
- **Causa (esperada)**: recusas reais de negócio (taxas dos simuladores: pagamento 0.85,
  estoque 0.9, notificação 0.95) que esgotaram o retry (máx 3) → `retry-limit-exceeded`.
  Com 120k pedidos, ~7% de FAILED é o esperado pelo modelo.
- **Como confirmar**: logs do orquestrador `action=fail-requested`/`retry-limit-exceeded`
  com `retry_count=3`; journal da saga mostra `PAYMENT_RESULT` `FAILED`.
- **Como resolver**: é comportamento de domínio. Para estudo, ajustar `approvalRate` no
  main dos workers; em produção, é o fluxo de falha legítimo (compensação/refund para
  pagamento).

### 3.4 Lag alto no Kafka (consumer atrás)
- **Sintoma**: `kafka-consumer-groups.sh --describe` mostra `LAG` grande.
- **Causa**: consumer do grupo processando mais devagar que a publicação do relay.
- **Como resolver**:
  1. `SAGA_WORKERS` (concorrência intra-instância) no serviço atrasado.
  2. Réplicas do serviço (KEDA por lag no K8s; `cmd/autoscaler` no compose).
  3. Checar se o watchdog anti-stall reconectou (`phase=reconnect` nos logs).

### 3.5 Throughput de processamento caiu
- **Sintoma**: `rate(saga_events_processed_total[60s])` despenca.
- **Causa provável**: Postgres saturado (journal gigante: 120k sagas ≈ 600k+ linhas em
  `saga_events`) ou CPU/memória do host no teto.
- **Como confirmar**: `docker stats` (postgres 30%+ CPU), `docker-compose logs postgres`.
- **Como resolver**: aguardar a drenagem (o volume é finito); em produção, escalar o
  Postgres (RDS) e revisar índices.

### 3.6 Relay/consumers OOM ou `no space left on device`
- **Sintoma**: container morre; build falha.
- **Causa**: disco da VM docker cheio (imagens/volumes acumulados) ou memória do host.
- **Como resolver**: `docker system prune -af`, `docker volume prune`, `.dockerignore`
  excluindo `terraform/` (732MB) e `docs/` (build context pequeno).

---

## 4. Resultado do teste de referência (120.000 pedidos, ambiente local)

- **Ingestão**: 120.000 pedidos em 242 s → **~496 ev/s** (teto do produtor local, 1 broker).
- **Processamento**: pico de **~2.350 ev/s** de eventos de domínio (o pipeline acompanha a
  carga; o limitante é a ingestão do relay, não o processamento).
- **Gargalo identificado**: `outbox-relay` a ~500 ev/s (1 réplica). Com **2 réplicas**, a
  drenagem da outbox acelera (~1.000 ev/s) — o `SKIP LOCKED` evita duplicação.
- **Estado após ~11 min** (com 2 relays):
  `PAYMENT_PENDING 66.823 · PAYMENT_APPROVED 33.721 · INVENTORY_RESERVED 7.672 · FAILED 8.319`
  → o pipeline estava no meio da drenagem; `FAILED` ~7% é recusa real (taxa 0.85).
- **Kafka**: lag baixo (orders.created=0) — confirma que a fila real é a outbox.

### Conclusões
1. O pipeline **processa mais rápido do que o relay consegue publicar** → para 2.000 ev/s
   sustentados de entrada, o ambiente precisa de **≥4 réplicas de relay** (ou `OUTBOX_BATCH_SIZE`
   maior) e, em produção, Kafka com mais brokers/partições.
2. O design de resiliência funciona: nada se perde (mensagens retidas no Kafka/outbox),
   as sagas convergem, e o escalonamento do relay é seguro (SKIP LOCKED).


## 5. Comparativo pós-otimização (A/B — 120k, depois das melhorias)

> Após as otimizações (migration `000006` índices+autovacuum, pool `DATABASE_POOL_*`,
> `OUTBOX_BATCH_SIZE=2000`), o mesmo teste de 120k foi repetido. **O load-generator
> falhou com timeout de I/O no Kafka broker (~75k publicados)** — descoberta que
> provou que as otimizações **moveram o gargalo do relay para o Kafka single-broker**.

| Métrica (mesma janela de tempo) | ANTES | DEPOIS | Δ |
|---|---|---|---|
| Processamento (pico) | ~2.350 ev/s | **~3.780 ev/s** | **+60%** |
| Outbox pendente durante a publicação | ~70k–117k | **~2k–3k** | **-97%** |
| Outbox no fim do teste | ~114k pendentes | **~51** | drenou |
| Gargalo identificado | `outbox-relay` (~500 ev/s) | **Kafka broker single** (timeout no producer) | mudou! |

### Leitura
1. As otimizações **funcionaram**: o relay com `batch=2000` drenou a outbox quase em
   tempo real e o pipeline processou ~60% mais eventos/s (pool + índices + autovacuum).
2. O **novo gargalo é o broker Kafka** (1 container no Colima, CPU limitada): a ingestão
   (~496 ev/s) + consumo acelerado (~3.700 ev/s) + commits saturaram o broker, e o
   produtor estourou o timeout (`i/o timeout` no `WriteMessages`).
3. **Para a Fase 10**: com Kafka multi-broker (MSK/Strimzi) o producer deixa de ser o
   limite; no local, o teto prático de ingestão caiu para ~75k pedidos por rodada com a
   configuração otimizada (ou use `KAFKA_ACKS=one` no load-generator para aliviar o broker).

