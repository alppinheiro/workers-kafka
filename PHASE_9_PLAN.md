# Fase 9: Kubernetes (kind + Helm + KEDA)

> Plano de execução. Contexto: `EVOLUTION_PLAN.md` (roadmap) e Fase 8 concluída
> (CI/CD valida e publica 9 imagens em `ghcr.io/alppinheiro/workers-kafka-<svc>`).
> Objetivo: rodar a stack em **Kubernetes local (kind)** com Helm e autoscaling via
> **KEDA por lag do Kafka**, validando o design antes da cloud (Fase 10 — EKS).

## 1. Objetivo

Subir o pipeline completo (orquestrador + 3 workers + outbox-relay + projector +
order-status + metrics-exporter) em **Kubernetes local (kind)**, gerenciado por **Helm**,
com:

- Deployments + Services + ConfigMap/Secret versionáveis (chart)
- Kafka e Postgres **rodando no cluster**
- **KEDA** escalando por consumer lag (a mesma métrica/lógica do autoscaler local)
- **Probes** de liveness/readiness e `resources` (requests/limits)
- **Migrations como Kubernetes Job**
- **Smoke end-to-end no cluster** (saga até `COMPLETED`)

Validação local com **custo zero** antes de gastar com EKS.

## 2. Arquitetura no cluster

```
kind cluster (2 nodes)
 └── namespace: order-saga
     ├── order-saga-orchestrator         Deployment (1..N réplicas via KEDA)
     ├── order-saga-worker-payment       Deployment
     ├── order-saga-worker-inventory     Deployment
     ├── order-saga-worker-notification  Deployment
     ├── order-saga-outbox-relay         Deployment
     ├── order-saga-projector            Deployment
     ├── order-saga-order-status         Deployment
     ├── order-saga-metrics-exporter     Deployment
     ├── order-saga-migrations           Job (imagem migrate/migrate)
     ├── create-order                    one-shot (Job com args ORDER_ID)
     ├── kafka (Strimzi)                 CRD Kafka + KafkaTopic (4 partições)
     ├── postgres (bitnami ou Deployment) Service + PVC
     ├── keda                            ScaledObject por consumer lag
     └── kube-prometheus-stack           Prometheus + Grafana + kube-state-metrics
```

**Imagens:** já publicadas pelo CI (Fase 8) — o cluster puxa de `ghcr.io`.
**Config:** ConfigMap (não-sensíveis) + Secret (`DATABASE_URL`), o mesmo contrato 12-factor
do `.env.example`/compose — sem recompilar.

## 3. Etapas

### 9.1 — kind local + Makefile
- Cluster kind (1 control-plane + 1 worker) via `kind`; ferramentas: `kubectl`, `helm`,
  `kind` (instalar via brew/go install).
- Targets novos no `Makefile`: `make k8s-up`, `make k8s-down`, `make k8s-logs`,
  `make k8s-smoke`.
- **Atenção de recurso:** kind roda nodes como containers no Colima (2 CPU/4 GB) —
  monitorar memória; default com réplicas mínimas.

### 9.2 — Helm chart `deploy/helm/order-saga`
- Templates parametrizados por serviço (Deployment + Service) com `values.yaml`
  (imagem/tag, réplicas, resources, probes, env) e `values-dev.yaml`/`values-prod.yaml`.
- ConfigMap com as variáveis de ambiente não-sensíveis; Secret com `DATABASE_URL`.

### 9.3 — Infra no cluster (Kafka + Postgres)
- **Kafka via Strimzi** (operador): CRDs `Kafka` e `KafkaTopic` — tópicos declarados com
  4 partições (Infra-as-Code no K8s; e é o caminho mais barato p/ cloud também).
- **Postgres**: chart `bitnami/postgresql` (ou Deployment + PVC simples com a mesma
  imagem `postgres:16-alpine`) — RDS só na Fase 10.
- **Migrations**: Kubernetes **Job** (imagem `migrate/migrate`) com `restartPolicy: OnFailure`.

### 9.4 — Probes e resources
- Os serviços não expõem HTTP de health → **adicionar `/healthz`** no mesmo server do
  `/metrics` (`metrics.Serve`) e usar liveness/readiness TCP/HTTP.
- `resources.requests/limits` por serviço (binários ~12 MiB — valores conservadores).

### 9.5 — KEDA (autoscaling por lag)
- Instalar KEDA via Helm no kind.
- `ScaledObject` com trigger `kafka` (topic, consumerGroup, `lagThreshold` ≈ 200 —
  mesma política do autoscaler local `AUTOSCALE_HIGH_LAG=200`), `minReplicas=1`,
  `maxReplicas=3` para orquestrador e workers.
- Validar escala 1 → 3 sob carga (load-generator) e volta a 1 com lag zero.

### 9.6 — Smoke no cluster
- `make k8s-smoke`: aplicar o chart → aguardar Ready → rodar Job `create-order` →
  verificar no Postgres do cluster (sagas `COMPLETED/FAILED`, journal, outbox drenada)
  e no read model (`order_views`).

### 9.7 — Observabilidade
- `kube-prometheus-stack` (Prometheus + Grafana + kube-state-metrics) no kind.
- Importar o dashboard "Saga - Visão Geral" (dashboard do projeto) no Grafana do cluster.

## 4. Decisões de arquitetura

| Tema | Escolha | Por quê |
|---|---|---|
| Kafka no K8s | **apache/kafka (manifesto local)** — fallback do Strimzi | Strimzi (quay.io) e bitnami (Docker Hub) bloqueados neste ambiente; `apache/kafka:3.9.0` é a mesma imagem do compose |
| Postgres | Deployment + PVC (postgres:16-alpine) | grátis; RDS na Fase 10 |
| Probes | `/healthz` no server de métricas + TCP | serviços não têm HTTP health hoje |
| Autoscaling | **KEDA** trigger `kafka` (lag) | mesma métrica do autoscaler local |
| Deploy | **Helm**; ArgoCD fica para a Fase 10 | Helm primeiro (fundação); GitOps como bônus |
| Observabilidade | kube-prometheus-stack **adiado** | recursos do Colima (4 GB) — fazer na Fase 10/cloud |

## 5. Ordem de execução e critérios de pronto

| Etapa | Risco | Dependência | DoD |
|---|---|---|---|
| 9.1 kind + make | baixo | ferramentas instaladas | `kubectl get nodes` Ready |
| 9.2 chart | médio | 9.1 | `helm template` ok; Deployments Up |
| 9.3 infra | médio (Strimzi) | 9.2 | Kafka Ready + tópicos 4 partições; Postgres healthy; migrations Job Completed |
| 9.4 probes/resources | baixo | 9.2 | `/healthz` responde; pod restart sem espera; limits aplicados |
| 9.5 KEDA | médio | 9.3 | ScaledObject ativo; escala 1→3 sob lag |
| 9.6 smoke | médio | 9.3–9.5 | saga `k8s-smoke-*` chega a `COMPLETED/FAILED` no cluster |
| 9.7 observabilidade | baixo | 9.6 | dashboard "Saga - Visão Geral" no Grafana do kind |

## 6. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Colima 4 GB pode não aguentar kind + Kafka + Postgres + Prometheus | réplicas mínimas default; Strimzi single-broker; fallback p/ bitnami/kafka; `make k8s-down` libera recursos |
| Strimzi exige CPU/memória razoáveis | config single-node, requests modestos; monitorar via `kubectl top` |
| `kind` sem load balancer p/ Ingress | usar `kubectl port-forward` p/ Grafana/API |
| Imagens GHCR privadas (pull) | tornar os pacotes **públicos** (ação pendente da Fase 8) OU configurar `imagePullSecret` |
| `/healthz` é mudança de código | CI (Fase 8) valida; mudança mínima em `metrics.Serve` |

## 7. Fora de escopo desta fase (Fase 10)

- EKS + Terraform (VPC, node groups, IRSA) — Fase 10
- Kafka **MSK** gerenciado e Postgres **RDS** — Fase 10 (custos)
- ArgoCD / GitOps / deploy contínuo — Fase 10
- External Secrets Operator, Ingress + cert-manager em cloud — Fase 10

## 8. Entregáveis desta fase

1. `deploy/helm/order-saga/` — chart completo (Deployments, Services, ConfigMap, Secret, Job de migrations).
2. `deploy/k8s/` — kind config, Strimzi `Kafka`/`KafkaTopic`, KEDA `ScaledObject`, kube-prometheus-stack.
3. `Makefile`: `k8s-up/down/logs/smoke`.
4. Código: endpoint `/healthz` no server de métricas (mudança pequena, validada pelo CI).
5. `EVOLUTION_PLAN.md`/README: Fase 9 registrada + runbook K8s.

## 9. Resultado (24/08/2026) — pipeline rodando no Kubernetes

**✅ Validado no cluster kind local (`order-saga`):**

| Etapa | DoD | Resultado |
|---|---|---|
| 9.1 kind + make | nodes Ready | ✅ `k8s-up/down/logs/smoke` funcionando |
| 9.2 Helm chart | `helm template` + Deployments Up | ✅ 8 Deployments + 7 Services + ConfigMap + Secret + Job migrations + 4 ScaledObjects |
| 9.3 infra | Kafka Ready + tópicos + Postgres + migrations | ✅ Kafka `apache/kafka:3.9.0` (KRaft), **10 tópicos** (4 partições) via Job `kafka-init`, 2 Postgres, migrations **Completas** |
| 9.4 probes | `/healthz` responde | ✅ liveness/readiness HTTP em `/healthz` (porta de métricas) |
| 9.5 KEDA | escala por lag | ✅ orquestrador **1 → 3 réplicas** com lag > 200 (`266667m/200`); ScaledObjects READY/ACTIVE |
| 9.6 smoke | saga terminal no cluster | ✅ `k8s-smoke`: `COMPLETED`, journal 20, outbox 0, read model `COMPLETED` |

**Fallback documentado (Strimzi/bitnami → apache/kafka):**
- Strimzi: o ambiente não confia no TLS do **quay.io** (x509 unknown authority — testado no host e no node). 
- bitnami/kafka: Broadcom **não publica mais no Docker Hub** (`docker.io/bitnami/kafka:4.0.0-*` → not found).
- Solução: **Deployment + Service + PVC com `apache/kafka:3.9.0`** (mesma imagem/config do docker-compose) e **Job `kafka-init`** criando os tópicos (IaC). Em cloud (Fase 10): MSK ou Strimzi/CRD.

**Gotchas resolvidos durante a validação:**
- **Advertised listener** do Kafka deve ser o **FQDN** (`kafka.order-saga.svc.cluster.local`) — senão o client (KEDA em outro namespace) não reconecta.
- **Readiness do Kafka** por `tcpSocket` (o probe via `kafka-topics` criava deadlock: Service sem endpoints → client falha).
- **Data dir** do Kafka é `/tmp/kraft-combined-logs` (config da imagem) — montar o PVC lá; memória do broker: limites 1 Gi (512 Mi causava **OOMKilled** sob carga).
- **Portas de métricas** por serviço: projector=9105, metrics-exporter=9107, order-status **sem** metrics (sem probe) — a tabela inicial do chart estava errada.
- Migrations do **banco de leitura** precisam de 2º container no Job + ConfigMap próprio.

**Nota:** 9.7 (kube-prometheus-stack + dashboard) foi **adiado** — o Colima (4 GB) já opera no limite com kind + Kafka + Postgres + app + KEDA. As métricas já são expostas por pod (`/metrics`) e o dashboard "Saga - Visão Geral" pode ser importado na Fase 10 (cloud), onde há recursos.
