# Fase 5: Escalabilidade de Produção (Partições, Consumer Groups, Outbox Claims e Autoscaler)

> Plano crítico da Fase 5, **voltado para produção**. Regra de ouro: **não quebrar o que já
> está construído** (Fases 1–4). Consultar `PHASE_2_PLAN.md`, `PHASE_3_PLAN.md`, `PHASE_4_PLAN.md`.

## 1. Contexto e objetivo

Comparar o desempenho **sequencial vs concorrente** e **escalar automaticamente conforme a
carga de mensagens do orquestrador**, seguindo o **modelo de produção** (o mesmo usado com
Kubernetes/KEDA): o paralelismo vem das **partições do Kafka** e de **múltiplas instâncias**
(consumer groups), com um **autoscaler externo** que observa o lag e ajusta o nº de réplicas.

### Base medida na Fase 4

| Métrica | Valor (hoje) |
|---|---|
| Ingestão (load-generator) | ~47.000 eventos/s |
| Processamento por consumer | dezenas de eventos/s (1 partição, single-thread) |
| Gargalos | 1 partição por tópico + consumers single-threaded |

## 2. Modelo de escalabilidade de produção (Kafka + Saga)

```
              orders.created (N partições)
                     │
        ┌────────────┼─────────────┐
   consumer group "orchestrator"   │
   ┌────────┐  ┌────────┐  ┌────────┐
   │ réplica 1 │  │ réplica 2 │  │ réplica 3 │   ← rebalanceamento automático do Kafka
   │ (part 0)  │  │ (part 1)  │  │ (part 2)  │      (mais réplicas = mais paralelismo)
   └────────┘  └────────┘  └────────┘
        │              │             │
        └──────────────┼─────────────┘
                   saga + outbox
                       │
              outbox-relay (2+ réplicas com claims)
                       │
                   orders.payment (N partições) → workers (N réplicas) → ...
```

- **Ordem da saga** preservada: chave = `order_id` → mesma partição → mesma réplica.
- **Autoscaler externo** (análogo ao KEDA/HPA): observa o **lag** do consumer group e
  ajusta o nº de réplicas via `docker-compose up -d --scale`.

## 3. Decisões de arquitetura

### 3.1 Partições N uniformes
- Todos os tópicos do fluxo com o **mesmo N** (ex.: 4) para manter a correlação por `order_id`
  (o hash do producer resolve para a mesma partição em todos os tópicos).
- Mudança **só de infra** (`kafka-init`); sem alterar domínio.

### 3.2 Consumer groups multi-instância (o padrão de produção)
- O paralelismo horizontal vem de **réplicas** do mesmo serviço no mesmo consumer group.
- O Kafka **rebalanceia** as partições entre as réplicas; se uma cai, as demais assumem.
- Redelivery pós-rebalanceamento é coberto pela **idempotência por `event_id`** (Fase 3).

### 3.3 Worker pool por partição (paralelismo intra-instância, opcional)
- `SAGA_WORKERS` = nº de Readers no **mesmo consumer group** dentro da instância
  (ex.: 2 goroutines = 2 consumidores no grupo). O Kafka distribui as partições entre eles.
- `SAGA_WORKERS=1` preserva **exatamente** o comportamento atual.
- Na prática de produção isso permite uma instância aproveitar várias partições em paralelo.

### 3.4 Outbox-relay com claims (escala horizontal segura)
- `FetchPending` com **`FOR UPDATE SKIP LOCKED`** → duas réplicas do relay **nunca** processam
  a mesma linha. Escalar `--scale outbox-relay=2` sem duplicar publicações.

### 3.5 Autoscaler externo (análogo ao KEDA/HPA)
- Novo comando `cmd/autoscaler` (roda no host): monitora o **lag** dos consumer groups via
  admin do Kafka a cada intervalo.
- Política com histerese:
  | Condição | Ação |
  |---|---|
  | `lag > AUTOSCALE_HIGH_LAG` por 2 checagens | `docker-compose up -d --scale <svc>=N+1` (até `MAX`) |
  | `lag == 0` por `AUTOSCALE_IDLE_SECONDS` | `--scale <svc>=N-1` (até `MIN`) |
- Alvos: `orchestrator`, `worker-payment`, `worker-inventory`, `worker-notification`, `outbox-relay`.
- Em produção real, esse papel é do **KEDA ScaledObject** (Kubernetes); aqui o autoscaler
  simula o mesmo comportamento na stack docker-compose.

### 3.6 Ordem por `order_id` (invariante central)
- Kafka garante ordem **por partição**; o mesmo `order_id` está sempre na mesma partição.
- Partições N iguais + chave `order_id` = correlação preservada independentemente de quem
  consome cada partição.

## 4. Etapas de implementação (incrementais e validadas)

### Etapa 5.1 — Partições N (infra)
- `kafka-init`: criar os 5 tópicos (+ DLQ) com N partições (mesmo N, ex.: 4).
- **Validação**: `make check` + e2e (fluxo feliz) + load test básico (nada quebra com N>1).

### Etapa 5.2 — Consumer groups multi-instância (manual primeiro)
- Confirmar que os serviços Go **não têm `container_name`** (são escaláveis) e usam consumer
  groups (já usam).
- **Validação**: `docker-compose up -d --scale orchestrator=2 --scale worker-payment=2` e
  observar o **rebalanceamento** (partições distribuídas) + e2e sem duplicação (idempotência).

### Etapa 5.3 — Worker pool por partição (intra-instância, opt-in)
- `ConsumerConfig.Workers` (env `SAGA_WORKERS`); default `1` = comportamento atual.
- `Consume` cria N Readers no mesmo grupo (goroutines), cada um com o loop atual (trace, DLQ,
  commit). DLQ writer é thread-safe; shutdown via `WaitGroup`.
- **Validação**: e2e com `SAGA_WORKERS=1` (idêntico ao atual) e `SAGA_WORKERS=3` (mesmo
  resultado, mais rápido); testes unitários do roteamento.

### Etapa 5.4 — Outbox-relay com claims
- `FetchPending` com `FOR UPDATE SKIP LOCKED` (uma tx: reserva linhas e devolve).
- **Validação**: `--scale outbox-relay=2` + carga → sem duplicação no Kafka (contar eventos).

### Etapa 5.5 — Autoscaler externo
- `cmd/autoscaler`: admin do Kafka (kafka-go) para ler lag dos grupos; política de histerese;
  executa `docker-compose up -d --scale`.
- Config: `AUTOSCALE_SERVICE`, `MIN`, `MAX`, `HIGH_LAG`, `IDLE_SECONDS`, `CHECK_INTERVAL`.
- **Validação**: load test (2.000+) → observa o autoscaler subindo réplicas e drenando o backlog.

### Etapa 5.6 — Benchmark comparativo
- Cenários: `1 réplica/1 worker` vs `3 réplicas` vs `autoscaler`.
- Métricas: sagas completas/s, tempo de dreno do backlog, pico de lag.
- Registrar em `BENCHMARK.md`.

## 5. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Quebrar ordem da saga | Chave `order_id` + partições N uniformes; validação de fora-de-ordem ativa |
| Duplicação em rebalanceamento | Idempotência por `event_id` (Fase 3) |
| 2 relays processarem a mesma outbox | `FOR UPDATE SKIP LOCKED` (claims) |
| Quebrar testes unitários | Concorrência é opt-in (`SAGA_WORKERS=1` = atual); domínio não muda |
| `--scale` em runtime (compose v1) | Testar antes; fallback: subir/descer manualmente documentado |
| Overload dos simuladores | `MAX` réplicas limita o teto |

## 6. Fora de escopo nesta fase

- Kubernetes/KEDA real (documentado como destino; o autoscaler é o análogo local).
- Sharding de banco / réplicas de leitura do Postgres.
- Métricas Prometheus/Grafana.

## 8. Visão de Planejamento Futuro (após a Fase 5)

### Cloud readiness (preparação para subir em cloud)
- O código é **agnóstico de infraestrutura**: mesma imagem Docker, config por env.
- Futuro: **Helm charts / K8s manifests** (Deployment + Service + **HPA/KEDA ScaledObject** por lag)
  e **Terraform** (Kafka gerenciado — MSK/Confluent, Postgres — RDS).
- O **autoscaler local** usa a mesma métrica (lag) que o KEDA usaria → a lógica já é validada localmente.

### Testes de containers / integração automatizada
- **Testcontainers (Go)**: testes de integração que sobem **Kafka + Postgres reais** em containers
  (fecha a pendência da Fase 1). Cobre: consumer/producer reais, fluxo ponta a ponta, DLQ.
- **Smoke tests do compose**: healthchecks + script de validação pós-`make up`.

### Métricas e Dashboards (Fase 6 proposta)
- Expor **métricas Prometheus** por serviço: eventos processados, latência, **lag do consumer
  group**, **profundidade da outbox**, erros/DLQ.
- Adicionar **Prometheus** e **Grafana** ao docker-compose com dashboards (throughput, lag,
  latência, fila da outbox).
- Complementa o **Jaeger** (traces) já existente.

### Evoluções transversais já planejadas
- **API REST** de consulta de pedido lendo o read model `order_views`.
- **Transação atômica** (estado + journal + outbox na mesma transação) — elimina janelas residuais.
- **Escala horizontal real** em cloud (K8s), com o autoscaler validado localmente.
- Logs estruturados (ex.: Loki) se necessário.

### Publicação na AWS (futuro)
- **EKS** (Kubernetes gerenciado) ou **ECS Fargate** para os serviços; **MSK** (Kafka gerenciado)
  ou MSK Serverless; **RDS PostgreSQL** (ou Aurora).
- O **autoscaler local (lag)** vira **KEDA ScaledObject** no EKS (mesma métrica/lógica).
- Mesma imagem Docker; configuração por env/segredos (SSM/Secrets Manager).

### CI/CD com GitHub Actions (planejamento)
> ⚠️ **Nota histórica (24/08/2026):** este planejamento foi **implementado na Fase 8**
> (`PHASE_8_PLAN.md`, `.github/workflows/ci.yml`) com 4 jobs — `check`, `integration`
> (Testcontainers), `smoke` (e2e) e `build-images` (9 imagens → **GHCR**, não ECR). O CD
> (Helm/K8s) permanece para as Fases 9/10.
- **CI** (`.github/workflows/ci.yml`):
  - `make check` (fmt, build, vet, lint) e testes unitários.
  - **Testes de integração** usando os **services nativos do GitHub Actions** (containers
    `postgres` e Kafka com healthchecks) — mesmo `DATABASE_URL` dos testes guardados da Fase 2/3.
  - Build da imagem Docker e push para **ECR** (tag por SHA do commit).
- **CD** (futuro): deploy no EKS via **Helm** (ou GitOps com ArgoCD) atualizando a imagem.
- **Nesta Fase 5**: criar o workflow de **CI** funcional (validação automática em cada push),
  mantendo o CD documentado para a etapa de cloud/AWS.

<!-- AWS_CI -->

### Ordem sugerida após a Fase 5
1. **Fase 6**: métricas Prometheus + dashboards Grafana.
2. **Testcontainers** (fechar integração automatizada da Fase 1).
3. **CI/CD**: GitHub Actions (CI funcional) + **cloud readiness AWS** (Helm + Terraform + EKS/MSK/RDS).
4. Evoluções (API REST, transação atômica).



## 9. Definição de Pronto (resumo da Fase 5)

1. `make check` verde e e2e idêntico ao atual com 1 partição/1 worker.
2. Multi-instância rebalanceia sem duplicar (validado com carga).
3. Outbox-relay escala horizontalmente sem duplicar.
4. Autoscaler responde ao lag (subir/descer réplicas observável).
5. Benchmark sequencial vs concorrente vs autoscaler registrado.


