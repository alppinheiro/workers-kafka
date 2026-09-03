# ☸️ Kubernetes — Guia de Comandos do Projeto

> **Runbook prático** para trabalhar no projeto **Order Saga Microservices** no
> **Kubernetes local (kind)** + **EKS (Fase 10)**: pré-requisitos, identidade do cluster,
> subir/atualizar/validar, comandos `kubectl`/`helm` por camada (app, Kafka, Postgres,
> KEDA), observação do pipeline e troubleshooting.
>
> **Leitura complementar:** `README.md` (visão geral + quick start), `docs/MANUAL.md`
> (§9 ambientes e §13 runbook), `PHASE_9_PLAN.md` (validação no kind) e `Makefile`
> (alvos `k8s-*`).

## Sumário

1. [Visão Geral do Ambiente](#1-visão-geral-do-ambiente)
2. [Pré-requisitos e Verificação](#2-pré-requisitos-e-verificação)
3. [Contextos: kind vs EKS (não erre o cluster!)](#3-contextos-kind-vs-eks-não-erre-o-cluster)
4. [Fluxo de Trabalho Diário](#4-fluxo-de-trabalho-diário)
5. [Subir o Ambiente do Zero (kind)](#5-subir-o-ambiente-do-zero-kind)
6. [O Que Existe no Cluster](#6-o-que-existe-no-cluster)
7. [Comandos kubectl Essenciais](#7-comandos-kubectl-essenciais)
8. [Helm e KEDA](#8-helm-e-keda)
9. [Kafka Dentro do Cluster](#9-kafka-dentro-do-cluster)
10. [PostgreSQL (escrita/leitura)](#10-postgresql-escritaleitura)
11. [Aplicação: Pedidos, Smoke, Logs, Healthz e Métricas](#11-aplicação-pedidos-smoke-logs-healthz-e-métricas)
12. [Atualizar as Imagens da Aplicação (`make k8s-images`)](#12-atualizar-as-imagens-da-aplicação-make-k8s-images)
13. [Troubleshooting](#13-troubleshooting)
14. [Parar, Limpar e Recriar](#14-parar-limpar-e-recriar)
15. [Cheatsheet Rápido](#15-cheatsheet-rápido)

---

## 1. Visão Geral do Ambiente

- **Cluster kind local:** `order-saga` (1 control-plane + 1 worker), criado via
  `deploy/k8s/kind-config.yaml`. Os nodes são containers rodando no **Colima/Docker**.
- **Namespace:** `order-saga` (todos os recursos da app + Kafka + Postgres).
- **Imagens da app:** `ghcr.io/alppinheiro/workers-kafka-<svc>:latest`
  (publicadas pelo CI — Fase 8). O nó **cacheia** as imagens; por isso existe o
  `make k8s-images` (veja §12).
- **Infra no cluster:** Kafka `apache/kafka:3.9.0` (KRaft single-node) + PostgreSQL
  16 (escrita `saga` e leitura `saga_read`).
- **Orquestração:** Helm chart `deploy/helm/order-saga` + **KEDA** (autoscale por
  consumer lag do Kafka).
- **Cloud (Fase 10):** EKS + Terraform em `terraform/` (nunca rodar comandos deste
  guia contra o EKS por engano — veja §3).

Layout dos recursos dentro do cluster:

```
namespace: order-saga
 ├── create-order (one-shot, sob demanda)
 ├── orchestrator / worker-payment / worker-inventory / worker-notification
 ├── projector (read model) / outbox-relay / order-status / metrics-exporter
 ├── kafka (apache/kafka KRaft) + Job kafka-init (tópicos)
 ├── order-saga-postgres (escrita) e order-saga-postgres-read (leitura)
 └── Jobs de migração (order-saga-migrations)
namespace: keda (operador KEDA + ScaledObjects para os consumers)
```

---

## 2. Pré-requisitos e Verificação

Ferramentas (instalar via Homebrew no macOS):

```bash
brew install kind kubernetes-cli helm colima docker
```

Verificação rápida:

```bash
command -v kind kubectl helm colima docker
kind version          # ex.: v0.32.0
kubectl version --client
helm version --short
colima status         # precisa estar "Running" (o kind roda dentro do Colima)
```

Se o Colima estiver parado:

```bash
colima start          # sobe a VM do Docker; nodes do kind voltam junto
docker context ls     # contexto "colima" deve ser o atual
```

---

## 3. Contextos: kind vs EKS (não erre o cluster!)

O `kubeconfig` pode conter **dois contextos**:

- `kind-order-saga` → cluster **local** (kind)
- `arn:aws:eks:...` → cluster **cloud** (EKS, Fase 10 — cuidado!)

**Sempre confira o contexto atual antes de qualquer comando destrutivo** (`helm`,
`rollout`, `delete`, `kubectl apply`):

```bash
kubectl config get-contexts        # lista contextos (o atual tem "*")
kubectl config current-context     # ex.: kind-order-saga
kubectl config use-context kind-order-saga   # troca para o local
```

> ⚠️ **Regra de ouro:** comandos do fluxo local (`make k8s-*`, `helm upgrade/install`,
> `kubectl delete`) **só** com o contexto `kind-order-saga`. Os alvos novos do Makefile
> (`k8s-images`) têm guarda automática e abortam se o contexto não for o kind.

Identidade do ambiente para conferir rapidamente:

```bash
kubectl cluster-info | head -1
kubectl get nodes                  # order-saga-control-plane + order-saga-worker
```

---

## 4. Fluxo de Trabalho Diário

Depois que a máquina reinicia (o cluster kind **continua existindo**, mas os pods
voltam com as imagens **em cache** no nó — possivelmente defasadas):

```bash
colima start                                   # 1. sobe o Docker/kind
kubectl config use-context kind-order-saga     # 2. contexto local (nunca AWS)
make k8s-images                                # 3. re-puxa as imagens :latest + restart (com warm-up)
make k8s-smoke ORDER_ID=k8s-smoke              # 4. valida e2e (saga até COMPLETED/FAILED)
```

Consultas e logs durante o trabalho:

```bash
kubectl -n order-saga get deploy,pods -o wide
kubectl -n order-saga logs deploy/order-saga-orchestrator --tail=100
make k8s-logs SVC=orchestrator                 # atalho p/ logs -f de um deployment
make k8s-smoke ORDER_ID=meu-pedido             # cria pedido e valida o pipeline
```

> **Por que o passo 3 importa:** o nó kind guarda as imagens baixadas; sem forçar o
> re-pull, você pode continuar rodando binários **antigos** (foi o que causou o bug de
> consumidores travados com imagens de 24/08). Detalhes em §12.

---

## 5. Subir o Ambiente do Zero (kind)

Se o cluster **não existir** (primeira vez ou após `make k8s-down`):

```bash
make k8s-up
```

O `make k8s-up` faz: `kind create cluster` (com `deploy/k8s/kind-config.yaml`) →
namespace → operador **KEDA** → Postgres/Kafka (`deploy/k8s/*.yaml`) → tópicos via Job
`kafka-init` → ConfigMaps de migrations → **Helm chart** da app (com
`image.pullPolicy=Always`).

> ⚠️ **Atenção (main atual):** o `make k8s-up` foi validado na Fase 9 (commit
> `c0c9f2f`). No `main` atual o chart evoluiu para o **EKS/GitOps** (Kafka dentro do
> chart com `storageClassName: gp2`, Secret removido do chart). Em cluster **reutilizado**
> prefira sempre o fluxo da §4 (`k8s-images`). Se for recriar do zero e algo falhar no
> `helm upgrade`, rode o chart com os valores locais e confira os itens abaixo.

**Pós-criação manual (caso o Secret não exista):** os Deployments/migrations usam o
Secret `order-saga-secrets` (chaves `DATABASE_URL` / `DATABASE_URL_READ`), que **não é
versionado**:

```bash
kubectl create secret generic order-saga-secrets -n order-saga \
  --from-literal='DATABASE_URL=postgres://saga:saga@order-saga-postgres:5432/saga?sslmode=disable' \
  --from-literal='DATABASE_URL_READ=postgres://saga:saga@order-saga-postgres-read:5432/saga_read?sslmode=disable'
```

Confira a subida completa:

```bash
kubectl -n order-saga get deploy
kubectl -n order-saga get pods,svc,jobs,pvc,cm,secret
kubectl -n order-saga wait --for=condition=ready pod -l app.kubernetes.io/name=kafka --timeout=180s
kubectl -n order-saga get job kafka-init            # Complete
kubectl -n order-saga get job order-saga-migrations # Complete
```

---

## 6. O Que Existe no Cluster

| Deployment | Função | Imagem (`ghcr.io/alppinheiro/...`) | Porta metrics/healthz |
|---|---|---|---|
| `order-saga-orchestrator` | saga orquestrada (estado+journal+outbox em 1 TX) | `workers-kafka-orchestrator` | 9101 |
| `order-saga-worker-payment` | executa pagamento (simulador) | `workers-kafka-worker-payment` | 9102 |
| `order-saga-worker-inventory` | executa estoque (simulador) | `workers-kafka-worker-inventory` | 9103 |
| `order-saga-worker-notification` | executa notificação (simulador) | `workers-kafka-worker-notification` | 9104 |
| `order-saga-projector` | projeta `order_views` (banco de leitura) | `workers-kafka-projector` | 9105 |
| `order-saga-outbox-relay` | publica a outbox no Kafka (claims) | `workers-kafka-outbox-relay` | 9106 |
| `order-saga-order-status` | auditoria de eventos terminais | `workers-kafka-order-status` | — (sem metrics) |
| `order-saga-metrics-exporter` | métricas do banco | `workers-kafka-metrics-exporter` | 9107 |
| `kafka` | broker KRaft single-node | `apache/kafka:3.9.0` | 9092 (TCP) |
| `order-saga-postgres` | banco de escrita (`saga`) | `postgres:16-alpine` | 5432 |
| `order-saga-postgres-read` | banco de leitura (`saga_read`) | `postgres:16-alpine` | 5432 |

Outros recursos por namespace `order-saga`:

- **Services:** `kafka`, `order-saga-postgres`, `order-saga-postgres-read` e um por
  serviço com metrics (`order-saga-orchestrator:9101`, ...).
- **Jobs:** `kafka-init` (cria tópicos) e `order-saga-migrations` (migrações).
- **PVCs:** `order-saga-kafka-pvc`, `order-saga-postgres-pvc`, `order-saga-postgres-read-pvc`
  (StorageClass `standard` do kind / local-path).
- **ConfigMaps:** `order-saga-config` (env não-sensível), `order-saga-migrations`,
  `order-saga-migrations-read`.
- **Secret:** `order-saga-secrets` (`DATABASE_URL`, `DATABASE_URL_READ`).
- **KEDA:** ScaledObjects `order-saga-orchestrator` + `order-saga-worker-*`
  (lag `>200`, min 1 / max 3).

---

## 7. Comandos kubectl Essenciais

Estado geral:

```bash
kubectl -n order-saga get all
kubectl -n order-saga get deploy,pods,svc,jobs,pvc,cm,secret
kubectl -n order-saga get pods -o wide          # IP/nó de cada pod
kubectl -n order-saga get deploy -o custom-columns='NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image,READY:.status.readyReplicas'
```

Logs:

```bash
kubectl -n order-saga logs deploy/order-saga-orchestrator --tail=100
kubectl -n order-saga logs deploy/order-saga-orchestrator -f
kubectl -n order-saga logs deploy/order-saga-orchestrator --previous   # último crash
# Com vários pods/replicas no mesmo deployment, filtre pelo label do serviço:
kubectl -n order-saga logs -l app.kubernetes.io/name=orchestrator --tail=100
```

Inspeção/execução:

```bash
kubectl -n order-saga describe pod <pod>
kubectl -n order-saga describe deploy order-saga-orchestrator
kubectl -n order-saga exec -it deploy/order-saga-postgres -- psql -U saga -d saga -c '\dt'
kubectl -n order-saga exec -it deploy/kafka -- /bin/sh
```

Rollout (aplicar imagem nova / reiniciar / desfazer):

```bash
kubectl -n order-saga rollout restart deployment order-saga-orchestrator
kubectl -n order-saga rollout status deployment order-saga-orchestrator
kubectl -n order-saga rollout undo deployment order-saga-orchestrator
```

> `kubectl logs deploy/<nome>` escolhe **um** pod do deployment (às vezes o mais antigo,
> ainda em Terminating durante um rollout). Em dúvida, use o filtro por label ou o pod
> explícito (coluna NAME de `kubectl get pods`).

---

## 8. Helm e KEDA

Releases instalados:

```bash
helm list -n order-saga    # release "order-saga" (chart local deploy/helm/order-saga)
helm list -n keda          # operador KEDA (kedacore/keda)
```

Renderizar o chart sem aplicar (útil para conferir o que o Helm criaria):

```bash
helm template order-saga deploy/helm/order-saga -n order-saga --set image.tag=latest
```

Upgrade do chart com parâmetros locais (imagem `:latest` sempre re-puxada):

```bash
helm upgrade --install order-saga deploy/helm/order-saga -n order-saga \
  --set image.tag=latest \
  --set image.pullPolicy=Always
```

KEDA (autoscale por lag do Kafka):

```bash
kubectl -n order-saga get scaledobject
kubectl -n keda get deploy,pods       # keda-operator, -metrics-apiserver, admission-webhooks
kubectl -n keda logs -l app.kubernetes.io/name=keda-operator --tail=50
kubectl -n order-saga describe scaledobject order-saga-orchestrator
```

> Se um `ScaledObject` aparecer `READY False` logo após subir, normalmente é porque o
> KEDA ainda não conseguiu ler lag do grupo (sem tráfego/offsets). Roda uma carga
> (`make k8s-smoke`) e reconsulte; o orquestrador costuma ficar `READY/ACTIVE True`.

---

## 9. Kafka Dentro do Cluster

O broker roda no Deployment `kafka` (KRaft single-node). Os scripts do Kafka ficam em
`/opt/kafka/bin/` dentro do container.

Tópicos (criados pelo Job `kafka-init`):

```bash
kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders.created
```

Tópicos principais: `orders.created`, `orders.payment.cmd/.result`,
`orders.inventory.cmd/.result`, `orders.notification.cmd/.result`, `orders.status`
(cada um com sua DLQ `orders.*.dlq`).

Consumer groups (lag por grupo = onde cada serviço está):

```bash
kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orchestrator

kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group worker-payment
```

Grupos esperados: `orchestrator`, `worker-payment`, `worker-inventory`,
`worker-notification`, `projector`, `order-status`.

Consumir mensagens de um tópico para diagnóstico (sem grupo — lê do início):

```bash
kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.created \
  --from-beginning --max-messages 5 --timeout-ms 15000
```

Publicar uma mensagem "crua" (diagnóstico):

```bash
kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders.created
```

> ⚠️ Os serviços **consomem pelo FQDN do Service**
> (`kafka.order-saga.svc.cluster.local:9092` no ConfigMap). Em diagnóstico, use
> `localhost:9092` dentro do pod do Kafka ou o Service `kafka:9092` de dentro de um pod
> no mesmo namespace.

**Tópicos "sumiram" (só resta `__consumer_offsets`)?** Recrie tudo com o mesmo Job do
repo:

```bash
kubectl -n order-saga delete job kafka-init --ignore-not-found
kubectl apply -f deploy/k8s/kafka.yaml     # recria Deployment/PVC/Service/Job kafka-init
kubectl -n order-saga wait --for=condition=complete job/kafka-init --timeout=120s
```

**Consumer group travado/offset defasado (reset completo):** desça os consumers, delete o
grupo e suba de novo (só se precisar reprocessar do zero):

```bash
kubectl -n order-saga delete scaledobject --all          # libera o min do KEDA
kubectl -n order-saga scale deploy order-saga-orchestrator \
  order-saga-worker-payment order-saga-worker-inventory \
  order-saga-worker-notification order-saga-projector order-saga-order-status \
  --replicas=0
kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --delete --group orchestrator
# ... repita para os demais grupos e volte a subir (replicas=1 + ScaledObjects)
```

---

## 10. PostgreSQL (escrita/leitura)

Bancos:

- **Escrita:** `saga` → tabelas `sagas`, `saga_events`, `outbox` (+ `schema_migrations`)
- **Leitura:** `saga_read` → tabelas `order_views`, `processed_events`

Acessar:

```bash
kubectl -n order-saga exec -it deploy/order-saga-postgres -- psql -U saga -d saga
kubectl -n order-saga exec -it deploy/order-saga-postgres-read -- psql -U saga -d saga_read
```

Consultas úteis (diagnóstico do pipeline):

```bash
# Onde o pedido está no fluxo
kubectl -n order-saga exec deploy/order-saga-postgres -- psql -U saga -d saga -c \
  "SELECT order_id, current_status, retry_count, transaction_id, updated_at
     FROM sagas ORDER BY updated_at DESC LIMIT 10;"

# "Trace de negócio" (journal)
kubectl -n order-saga exec deploy/order-saga-postgres -- psql -U saga -d saga -c \
  "SELECT id, component, direction, event_type, status_anterior, status_atual, created_at
     FROM saga_events WHERE order_id='meu-pedido' ORDER BY id;"

# Outbox pendente (o relay não está dando conta?)
kubectl -n order-saga exec deploy/order-saga-postgres -- psql -U saga -d saga -c \
  "SELECT count(*) AS pendentes FROM outbox WHERE published_at IS NULL;"

# Read model (banco de leitura)
kubectl -n order-saga exec deploy/order-saga-postgres-read -- psql -U saga -d saga_read -c \
  "SELECT order_id, current_status, last_event_type, last_event_at, transaction_id
     FROM order_views WHERE order_id='meu-pedido';"
```

---

## 11. Aplicação: Pedidos, Smoke, Logs, Healthz e Métricas

**Criar um pedido (one-shot) sem smoke:**

```bash
kubectl -n order-saga run create-order-meu-pedido \
  --image=ghcr.io/alppinheiro/workers-kafka-create-order:latest \
  --env=KAFKA_BROKERS=kafka:9092 --restart=Never -- meu-pedido

kubectl -n order-saga logs pod/create-order-meu-pedido   # "evento publicado" = ok
kubectl -n order-saga delete pod create-order-meu-pedido # limpeza
```

**Smoke end-to-end (recomendado):** cria o pedido, aguarda ~30s e valida
saga terminal + journal + outbox drenada + read model:

```bash
make k8s-smoke ORDER_ID=meu-pedido
# ou direto:
bash scripts/k8s-smoke.sh meu-pedido
```

Saída esperada (aceita `COMPLETED` **ou** `FAILED` — cenários determinísticos):

```text
sagas: COMPLETED
saga_events: 20
outbox pendente: 0
order_views: COMPLETED
=== K8S SMOKE OK ===
```

**Healthz** (porta de metrics de cada serviço — `order-status` não expõe):

```bash
kubectl -n order-saga exec deploy/order-saga-orchestrator -- wget -qO- http://localhost:9101/healthz
kubectl -n order-saga exec deploy/order-saga-outbox-relay -- wget -qO- http://localhost:9106/healthz
```

**Métricas Prometheus** (por pod):

```bash
kubectl -n order-saga port-forward deploy/order-saga-orchestrator 9101:9101
# em outro terminal:
curl -s localhost:9101/metrics | head -40
```

**Logs correlacionados** (JSON, com `order_id`, nas imagens atuais):

```bash
kubectl -n order-saga logs deploy/order-saga-orchestrator | grep 'meu-pedido'
kubectl -n order-saga logs deploy/order-saga-worker-payment   | grep 'meu-pedido'
```

---

## 12. Atualizar as Imagens da Aplicação (`make k8s-images`)

**Problema que motivou o comando:** o nó do kind **cacheia** as imagens baixadas. Ao
reutilizar o cluster (máquina reiniciada, cluster de dias atrás), os pods voltam com os
binários **antigos** — foi o que causou consumidores kafka-go travados com imagens de
24/08, mesmo com o Kafka/Postgres saudáveis.

**Solução:** manter as imagens da app sempre no `:latest` publicado pelo CI no GHCR e
forçar o re-pull a cada subida.

```bash
make k8s-images        # atualiza a app (GHCR :latest) no cluster existente
make k8s-refresh       # alias (mesma coisa)
```

O que ele faz:

1. **Guarda de contexto:** aborta se o contexto atual **não** for `kind-order-saga`
   (nunca roda contra a AWS).
2. Seta `imagePullPolicy=Always` nos 8 Deployments da app.
3. `kubectl rollout restart` → o kubelet **re-puxa** as imagens do GHCR.
4. Aguarda `rollout status`.
5. **Warm-up** (`K8S_WARMUP`, default `75s`): aguarda os consumers estabilizarem —
   o reader kafka-go pode iniciar "travado" e o watchdog anti-stall reconecta em
   ~45–90s (documentado no BENCHMARK.md). Desative com `K8S_WARMUP=0` se preferir.

```bash
make k8s-images K8S_WARMUP=0    # pula o warm-up (mais rápido; smoke logo após pode falhar por timing)
```

**Alternativas** (se o GHCR não estiver acessível/privado):

```bash
# build local e carga no kind (requer as imagens no docker local):
make rebuild
kind load docker-image ghcr.io/alppinheiro/workers-kafka-orchestrator:latest --name order-saga
# (na máquina atual, o kind load falhou com erro de blob no containerd; preferir o Always+pull)

# ou pinar por digest (imutável) no helm:
helm upgrade --install order-saga deploy/helm/order-saga -n order-saga \
  --set image.tag=sha-<sha>   # ver ghcr.io/alppinheiro/workers-kafka-<svc>:sha-<sha>
```

> Se você **mudou código localmente** e quer testar no cluster, o fluxo é: push →
> CI publica `:latest` → `make k8s-images`. Para iterar sem push, o caminho é o
> **Docker Compose** (`make up`) ou `kind load` após build local.

---

## 13. Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| `kubectl`/`helm` mexendo no lugar errado | contexto aponta para EKS | `kubectl config use-context kind-order-saga` |
| `Error from server: nodes "order-saga-control-plane" already exist` no `kind create` | cluster já existe | use o fluxo da §4 (`colima start` + `make k8s-images`), não `k8s-up` |
| Pod `CrashLoopBackOff` / `Error` | app sai no boot (env/DB/Kafka indisponível) | `kubectl logs deploy/order-saga-X --previous`; conferir ConfigMap `order-saga-config` e Secret `order-saga-secrets` |
| `CreateContainerConfigError` | Secret `order-saga-secrets` ausente | recriar (comando na §5) |
| `ImagePullBackOff` | imagem não existe/privada | conferir tag; `docker login ghcr.io`; usar `:latest` público ou `imagePullSecret` |
| Pod `Running` mas `0/1 Ready` logo após subir | app espera DB/Kafka e só abre `/healthz` depois (retry + ~50s) | aguardar 1–2 min e rever |
| `stall-detected ... fetches=0` nos logs do consumer | stall do kafka-go na subida (reader sem fetch) | aguardar o watchdog reconectar (~45–90s); se recorrente, `kubectl rollout restart deployment order-saga-X` |
| Mensagem publicada mas saga não anda | consumer em stall/rebalance na subida **ou** offset do grupo à frente | `describe --group orchestrator` (lag); aguardar; revalidar; último caso: reset do grupo (§9) |
| Smoke falha com `sagas:` vazio/`PAYMENT_PENDING` logo após refresh | timing (validou durante a janela de stall de um worker) | aguardar 1–2 min e rodar `make k8s-smoke` de novo (o pedido costuma completar mesmo assim) |
| Kafka com só `__consumer_offsets` (tópicos sumiram) | storage/metadados do Kafka corrompido/reset | recriar tópicos via Job `kafka-init` (§9) |
| `ScaledObject` `READY False` | KEDA sem lag/offsets ainda | rodar `make k8s-smoke` e reconsultar `kubectl -n order-saga get scaledobject` |
| Kafka morre com `137` (OOM) | limite de memória do broker | reduzir concorrência (`SAGA_WORKERS`), subir menos réplicas (docs/MANUAL.md §12.9) |
| Pod `Terminating` por muito tempo | nó lento / grace period | aguardar; `kubectl delete pod <pod> --force --grace-period=0` se persistir |
| Logs antigos aparecem em `kubectl logs deploy/...` | deployment com pod velho terminando | usar pod explícito ou `-l app.kubernetes.io/name=<serviço>` |

---

## 14. Parar, Limpar e Recriar

**Apenas parar o Colima** (cluster kind continua salvo):

```bash
colima stop
# depois: colima start + kubectl config use-context kind-order-saga + make k8s-images
```

**Derrubar a stack Kubernetes local** (remove Helm releases + cluster kind):

```bash
# IMPORTANTE: contexto kind-order-saga (o alvo remove o kind inteiro!)
make k8s-down
```

Equivalente manual:

```bash
helm uninstall order-saga -n order-saga
helm uninstall keda -n keda
kind delete cluster --name order-saga
```

**Recriar do zero:** veja §5. Depois de recriar, rode `make k8s-images` (se necessário)
e `make k8s-smoke ORDER_ID=k8s-smoke` para validar.

---

## 15. Cheatsheet Rápido

| Objetivo | Comando |
|---|---|
| Contexto local | `kubectl config use-context kind-order-saga` |
| Estado | `kubectl -n order-saga get deploy,pods,svc,jobs,pvc` |
| Logs de um serviço | `make k8s-logs SVC=orchestrator` (ou `kubectl -n order-saga logs deploy/order-saga-orchestrator`) |
| Atualizar imagens da app | `make k8s-images` |
| Smoke e2e | `make k8s-smoke ORDER_ID=meu-pedido` |
| Criar pedido avulso | `kubectl -n order-saga run create-order-x --image=ghcr.io/alppinheiro/workers-kafka-create-order:latest --env=KAFKA_BROKERS=kafka:9092 --restart=Never -- x` |
| Tópicos Kafka | `kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Lag dos consumidores | `kubectl -n order-saga exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orchestrator` |
| Postgres escrita | `kubectl -n order-saga exec -it deploy/order-saga-postgres -- psql -U saga -d saga` |
| Postgres leitura | `kubectl -n order-saga exec -it deploy/order-saga-postgres-read -- psql -U saga -d saga_read` |
| Healthz | `kubectl -n order-saga exec deploy/order-saga-orchestrator -- wget -qO- http://localhost:9101/healthz` |
| Reiniciar serviço | `kubectl -n order-saga rollout restart deployment order-saga-orchestrator` |
| KEDA | `kubectl -n order-saga get scaledobject` |
| Derrubar tudo | `make k8s-down` (contexto kind!) |
