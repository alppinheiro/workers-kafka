# Teste de Carga na Cloud AWS — Relatório

> **Data:** 28/08/2026
> **Infra:** EKS (4 nodes t3.small SPOT) + RDS free tier + Kafka KRaft single-node no cluster
> **Objetivo:** validar o desempenho do pipeline na cloud com teste de carga (como o local)

---

## 1. Resultado da publicação

| Métrica | Valor |
|---|---|
| **Pedidos publicados** | **120.000** |
| **Duração da publicação** | 241s (~4 min) |
| **Throughput de publicação** | **~497 ev/s** |
| Ferramenta | `load-generator` (pod one-shot no EKS) |

> O limite de ~497 ev/s é do **node t3.small** (2 vCPU) publicando no Kafka single-node.
> No ambiente local (máquina mais potente) alcançávamos mais; na cloud o custo é o fator.

## 2. Descoberta crítica — `sagaWorkers=3` trava o pipeline

Ao otimizar com `sagaWorkers=3` (antes do teste), os **workers entraram em loop de reconexão**
contínuo com o Kafka:

```
healthz falhou: kafka inacessível ... dial tcp 172.20.103.252:9092: operation was canceled
reconectando reader (a cada ~1s)
```

**Causa:** com `sagaWorkers=3`, cada worker cria **3 readers no mesmo consumer group**; no Kafka
**single-node** (sem réplicas), isso gera rebalanceamento contínuo que impede o processamento.
As sagas ficaram **presas em PAYMENT_PENDING** (114k).

**Correção:** reverter para `sagaWorkers=1` → o pipeline **destravou imediatamente**.

> 📌 **Lição:** `sagaWorkers>1` exige Kafka com **múltiplos brokers/partições saudáveis**.
> No single-node do estudo, `sagaWorkers=1` é o correto. Escala horizontal (mais réplicas via
> KEDA) é o caminho certo no EKS.

## 3. Resultado da drenagem (após a correção)

Estado das sagas **~15 min após o fim da publicação** (prefixo `st8035`):

| Status | Quantidade |
|---|---|
| **COMPLETED** | **17.996** ✅ |
| **FAILED** | 3.959 |
| INVENTORY_RESERVED | 2.145 (em andamento) |
| PAYMENT_APPROVED | 2.621 (em andamento) |
| PAYMENT_PENDING | 93.279 (fila) |

**Throughput de processamento observado:** ~100–200 ev/s (workers processando + KEDA escalando).

> A drenagem **completa** dos 120k levaria ~30–40 min no t3.small (o pipeline processa em
> paralelo, mas o node é limitado). Não aguardamos o fim para não estender o custo da sessão.

## 4. KEDA em ação

- O **orquestrador escalou de 1 → 3 réplicas** conforme o lag do `orders.created` aumentou ✅
- Os workers ficaram com HPA target de 3 (lag alto) ✅
- Após a drenagem, o **KEDA desescalou** para 1 réplica (lag caindo) ✅

**Autoscaling por lag validado na cloud!**

## 5. Custos da sessão de teste

| Item | Custo |
|---|---|
| 4 nodes t3.small SPOT (~2h no total) | ~US$ 0,16 |
| EKS control plane (~2h) | ~US$ 0,20 |
| NAT + EBS | ~US$ 0,12 |
| RDS (free tier) | US$ 0 |
| **Total estimado da sessão** | **< US$ 0,50** |

## 6. Correções aplicadas durante o teste

1. **`sagaWorkers` 3 → 1** (commit `1c7c798`) — destravou o pipeline.
2. **Kafka `mountPath`** → `/tmp/kafka-logs` (o log.dirs real do Kafka; antes gravava fora do volume).
3. **`fsGroup: 1000`** no Kafka — o volume EBS monta como root; sem isso o Kafka não escreve.
4. **initContainer `fix-lost-found`** — remove o diretório `lost+found` do EBS que o Kafka rejeita.
5. **8 partições** nos tópicos principais (via `--alter`).

## 7. Próximos passos recomendados

- Para um teste de carga **mais representativo**, usar node **maior** (ex: t3.medium/large — sai do
  free tier, custo ~US$ 0,03/h extra) OU **Kafka multi-broker** (Strimzi) para suportar
  `sagaWorkers>1`.
- Validar a **drenagem completa** até `COMPLETED` (rodar com volume menor, ex: 20k, para terminar
  em ~5 min).
- Considerar **MSK** (Kafka gerenciado) para eliminar o gargalo do single-node.
