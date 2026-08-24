# Plano de Evolução: Order Saga Microservices

Este documento descreve o roadmap técnico para transformar o projeto de estudo em uma implementação resiliente, observável e performática.

## 🗺️ Visão Geral do Roadmap

A evolução foi dividida em 5 fases sequenciais. A ordem é crítica: não otimizamos performance (Fase 5) sem antes ter testes (Fase 1) e observabilidade (Fase 4).

---

## 🛡️ Fase 1: A Rede de Segurança (Testes Automatizados)
**Objetivo:** Garantir que a lógica de negócio não regrida durante a evolução.

- [ ] **Testes Unitários:**
    - Validar a máquina de estados do \`orchestrator\`.
    - Testar a lógica de decisão de retry vs falha definitiva.
- [x] **Infraestrutura de Teste:**
    - Configurar `testcontainers-go` para subir Kafka real durante a execução dos testes (Kafka + Postgres reais — `make integration`, build tag `integration`).
- [ ] **Testes de Integração:**
    - Fluxo Feliz: `PENDING -> COMPLETED` (coberto nos containers — `TestSagaFlowWithPostgresContainer` e `TestKafkaProducerConsumerRoundTrip`).
    - Fluxo de Compensação: \`PAYMENT_APPROVED -> INVENTORY_FAIL -> PAYMENT_REFUNDED\`.
    - Fluxo de Retry: Validar que o sistema tenta X vezes antes de falhar.

## 💾 Fase 2: A Memória do Sistema (Persistência e Rastreabilidade)
**Objetivo:** Eliminar a perda de estado em caso de restart do orquestrador e registrar todos os eventos para rastreabilidade.

🔄 **Em execução** — ver `PHASE_2_PLAN.md` para o plano detalhado.

- [ ] **Definição de Tecnologia:**
    - Implementar PostgreSQL (Relacional) para salvar o estado da saga. *(decidido: PostgreSQL 16, driver `jackc/pgx/v5`)*
- [ ] **Camada de Persistência:**
    - Criar `internal/infrastructure/persistence` com interfaces de `SagaRepository`.
    - **Journal de eventos:** tabela `saga_events` (append-only) com `payload`, `request_payload` e `response_payload` dos gateways.
    - **Banco de leitura:** read model `order_views` alimentado por projeção via Kafka (serviço `projector`).
- [ ] **Refatoração do Orquestrador:**
    - Implementar o padrão: `Consome Evento -> Recupera Estado do DB -> Processa Lógica -> Salva Novo Estado -> Publica Próximo Evento`.
    - Workers também registram eventos `IN`/`OUT` e `GATEWAY_REQUEST`/`GATEWAY_RESPONSE`.
- [ ] **Migrations:**
    - `golang-migrate/migrate` via container no `docker-compose` (diretórios `migrations/` e `migrations-read/`).

## 🧱 Fase 3: O Escudo de Resiliência (Robustez e DLQ)
**Objetivo:** Garantir a estabilidade do pipeline diante de "poison pills" e duplicidades.

✅ **Concluída** — ver `PHASE_3_PLAN.md` para o plano detalhado.

- [x] **Dead Letter Queues (DLQ):**
    - Criar tópicos de erro (ex: `orders.payment.dlq`).
    - Mover mensagens que excederam o limite de retry para a DLQ em vez de descartá-las.
    - Erros definitivos (`ErrNonRetryable`) → DLQ + commit; erros transitórios → retry.
- [x] **Idempotência:**
    - Implementar a verificação de `event_id` ou `status_anterior` no DB para evitar processamento duplo de mensagens.
    - `EventLogRepository.Has(event_id, component)`: orquestrador e workers ignoram eventos cujo `IN` já foi registrado.
- [x] **Gestão de Erros:**
    - Diferenciar erros transitórios (Network) de erros definitivos (Business Logic) para decidir sobre o retry.
- [x] **Outbox Pattern:**
    - Tabela `outbox` no banco de escrita + `OutboxPublisher` + serviço `outbox-relay` que publica no Kafka.
    - Eliminar a dependência de publish direto durante o processamento.

## 👁️ Fase 4: Os Olhos do Sistema (Observabilidade Distribuída)
**Objetivo:** Rastrear pedidos em tempo real através de múltiplos workers.

✅ **Concluída** — ver `PHASE_4_PLAN.md` para o plano e como utilizar.

- [x] **Instrumentação OpenTelemetry:**
    - Adicionar SDK do OTel ao orquestrador, workers, projector, outbox-relay e create-order.
- [x] **Context Propagation:**
    - Implementar a passagem de `trace_id` via Kafka Headers (W3C `traceparent`).
    - Propagação preservada através da outbox (coluna `traceparent` + reconstrução no relay).
- [x] **Visualização:**
    - Adicionar container `Jaeger` ao `docker-compose.yml`.
    - Validar a visualização do grafo de chamadas de um único `order_id` (cadeia única).

## 🚀 Fase 5: O Motor de Alta Performance (Concorrência e Escala)
**Objetivo:** Maximizar o uso de CPU e aumentar a vazão de pedidos.

✅ **Concluída** — ver `PHASE_5_PLAN.md` e `BENCHMARK.md`.

- [x] **Processamento Concorrente:**
    - Implementar Worker Pools com Goroutines para processar mensagens de forma assíncrona (`SAGA_WORKERS`: Readers concorrentes no mesmo consumer group).
    - Implementar controle de concorrência (semáforos) para não sobrecarregar simuladores externos (`AUTOSCALE_MAX` como teto de réplicas).
- [x] **Otimização de Kafka:**
    - Aumentar número de partições nos tópicos (4 partições).
    - Configurar `Consumer Groups` para escala horizontal de workers (multi-instância via `--scale`).
- [x] **Benchmarking:**
    - Comparar métricas de `Pedidos/Segundo` da versão sequencial vs concorrente (`BENCHMARK.md`).
- [x] **Extras de produção:**
    - Outbox-relay com `FOR UPDATE SKIP LOCKED` (claims) para escala horizontal.
    - Autoscaler por lag (análogo local ao KEDA/HPA).

## 📊 Fase 6: Observabilidade de Métricas (Prometheus + Grafana)
**Objetivo:** Completar a observabilidade com métricas de throughput, latência, backlog e outbox.

✅ **Concluída** — ver `PHASE_6_PLAN.md`.

- [x] **Métricas Prometheus por serviço** (`/metrics` nas portas 9101–9107): eventos recebidos/processados/falhados/publicados, latência (histograma), DLQ.
- [x] **`metrics-exporter`**: gauges do Postgres (sagas por status, COMPLETED, FAILED) a cada 10s.
- [x] **Prometheus + Grafana no docker-compose** com dashboard provisionado "Saga - Visão Geral" (8 painéis).

## 🧪 Integração automatizada (Testcontainers)
**Objetivo:** Fechar a pendência da Fase 1 — testes com Kafka e Postgres reais em containers.

✅ **Concluída** — `make integration` (build tag `integration`).

- [x] **Round-trip Kafka**: `Producer` → `Consumer` com Kafka real em container.
- [x] **Fluxo completo da saga**: orquestrador + repositórios reais contra Postgres real (journal `saga_events` até `COMPLETED`).

---

## 🚀 Fase 7: Performance e Prontidão de Produção (planejada)
**Objetivo:** eliminar o gargalo do `outbox-relay` (benchmark revelou teto de ~48 ev/s) e garantir
fluxo contínuo + rastreabilidade total. Detalhes em `PHASE_7_PLAN.md`.

- [x] **7.1 Relay sem gargalo**: log agregado por ciclo (não por evento), loop contínuo com backoff
  (sem timer fixo de 1s), `OUTBOX_BATCH_SIZE` (default 500), `MarkPublished` em lote (`ANY($1)`),
  retenção/purga da outbox. *✅ Medido: ~50 → **~485 ev/s** (~9,7×).*
- [x] **7.2 Commit em lote + resiliência** (`KAFKA_COMMIT_BATCH`/`KAFKA_COMMIT_INTERVAL`; commit de
  offsets acumulados; `UnknownTopicOrPartition` como retry — não derruba mais; **watchdog anti-stall**
  que reconecta o reader em até 45 s). *✅ Orquestrador 216 ev/s; fluxo 2.000/2.000 em 60 s; sobrevive
  a tópico recriado e reconecta sozinho.*
- [x] **7.3 Rastreabilidade**: 
  - *Correção do Grafana*: dashboard não mostrava gráficos (UID do datasource não fixado) → `uid: Prometheus` no provisionamento; validado com 10 painéis via API.
  - *Gauge stale corrigido* (`ResetOrdersPending`); novas métricas `saga_consumer_lag{group,topic}` e `saga_outbox_max_age_seconds`; alertas `SagaDLQGrowth` + `SagaConsumerStalled` carregados.
  - Índice de correlação `saga_events(order_id, created_at)` já existia (migration 000002); journal como "trace de negócio" documentado.
- [x] **7.4 Transação atômica única** (estado + journal + outbox em 1 tx):
  - *`SagaUnitOfWork`*: porta `application.SagaTx` + `application.SagaUnitOfWork`; implementação `internal/infrastructure/uow` com `pgx.Tx` (commit em bloco; rollback em qualquer erro).
  - *Repositórios transacionais*: `DBTX` (contrato compartilhado por `*pgxpool.Pool` e `pgx.Tx`) + `NewSagaRepositoryTx`/`NewEventLogRepositoryTx`/`NewOutboxRepositoryTx`.
  - *Handlers atômicos*: orquestrador (`StartOrder`, `HandleResult`) e workers (pagamento/estoque/notificação) processam cada evento dentro de uma única transação — estado, journal e outbox são gravados juntos ou nenhum é gravado.
  - *Testes de consistência*: `TestUnitOfWork_AtomicCommit` (as 3 tabelas persistem juntas) e `TestUnitOfWork_AtomicRollback` (erro no bloco desfaz tudo) + versão Testcontainers (`TestUnitOfWorkRollbackWithContainer`); `make check` e `make integration` verdes.
- [x] **7.5 Validação final de produção (DoD)**:
  - *Benchmark A/B/C*: as 3 configurações drenam 3.000 pedidos em ~80 s (outbox=0); throughput ~258–261 ev/s — o pipeline deixou de ser o gargalo nesse volume. Detalhes em `BENCHMARK.md`.
  - *Resiliência*: R1 restart orquestrador (1.000/1.000 sagas drenadas), R2 tópico deletado (consumers sobrevivem), R3 relay duplicado (0 duplicatas — SKIP LOCKED), R4 worker caído (800/800 retomadas).
  - *Runbook operacional* no README + `scripts/benchmark.sh`; `make check` e `make integration` verdes.

---

## 🚀 Fase 8: CI/CD com GitHub Actions (concluída — falta só ação manual 8.5)

**Objetivo:** rede de segurança automática — toda mudança passa por `check` (qualidade +
unitários), `integration` (Testcontainers), `smoke` (e2e via docker-compose) e `build-images`
(push das 9 imagens para GHCR). Pré-requisito para deploy em Kubernetes (Fase 9). Detalhes em `PHASE_8_PLAN.md`.

- [x] **8.1** Job `check` — `make check` verde no runner (fmt, vet, build, testes, lint) + cache do Go.
- [x] **8.2** Job `integration` — Testcontainers (Kafka + Postgres reais) verdes no runner
  (ajuste do `DOCKER_HOST` do Makefile, que hoje aponta para o colima local).
- [x] **8.3** Job `smoke` — sobe o compose e valida saga `ci-smoke-*` até terminal (sagas + journal + outbox + read model).
- [x] **8.4** Job `build-images` — 9 imagens no GHCR (`ghcr.io/<owner>/workers-kafka-<svc>`), tags `:sha-<sha>`/`:latest` (e `:vX.Y.Z` em tags).
- [ ] **8.5** Branch protection em `main` (PR obrigatório + checks) e badge de status no README. *(ação manual no GitHub — badge já adicionado)*

---

## ☸️ Fase 9: Kubernetes local (kind + Helm + KEDA) (concluída — 9.7 adiado p/ cloud)

**Objetivo:** rodar a stack em **Kubernetes local (kind)** — Helm chart (`deploy/helm/order-saga`),
Kafka (`apache/kafka` KRaft + Job `kafka-init` de tópicos — fallback do Strimzi/bitnami), Postgres
no cluster, **KEDA** escalando por consumer lag, probes `/healthz` + resources, migrations como Job
e smoke end-to-end. Validou o design a custo zero antes da cloud (Fase 10). Detalhes em `PHASE_9_PLAN.md`.

- [x] **9.1** Cluster kind (2 nodes) + `Makefile` `k8s-up/down/logs/smoke`.
- [x] **9.2** Helm chart `order-saga` (Deployments/Services/ConfigMap/Secret/values por env).
- [x] **9.3** Infra no cluster: Kafka (`apache/kafka` KRaft + Job `kafka-init` de tópicos) + Postgres + migrations Job.
- [x] **9.4** Probes `/healthz` (mudança em `metrics.Serve`) + resources por serviço.
- [x] **9.5** KEDA `ScaledObject` por lag (min 1 / max 3) — orquestrador escalou 1→3 sob carga.
- [x] **9.6** Smoke no cluster: saga `k8s-smoke-*` até `COMPLETED/FAILED`.
- [ ] **9.7** Observabilidade (kube-prometheus-stack + dashboard) — **adiado** (recursos do Colima 4 GB); fazer na Fase 10/cloud.

---

## ☁️ Fase 10: Cloud AWS (EKS + Terraform + GitOps) (planejada)

**Objetivo:** subir o projeto em **produção na AWS** — EKS provisionado com **Terraform**,
imagens do GHCR (multi-arch), **KEDA** por lag, **ArgoCD (GitOps)** para deploy, Kafka e Postgres
gerenciados (ou self-hosted p/ reduzir custo) e observabilidade no cluster. Estratégia de custo:
**criar → estudar → destruir** (IaC). Detalhes em `PHASE_10_PLAN.md`.

- [ ] **10.1** Terraform: VPC + EKS (control plane + node group) + IRSA/IAM.
  *(✅ arquivos `terraform/` prontos e `terraform validate` OK — aplicar exige `aws configure`)*
- [ ] **10.2** Infra gerenciada: RDS Postgres (free tier) + Kafka (**MSK** ou **Strimzi** no EKS — decisão de custo).
- [ ] **10.3** Helm chart no EKS: deploy da stack + KEDA + Ingress (nginx) + secrets.
- [ ] **10.4** **ArgoCD** (GitOps): app do repositório apontando para o chart + auto-sync.
- [ ] **10.5** Observabilidade: kube-prometheus-stack + dashboard "Saga - Visão Geral" + alertas.
- [ ] **10.6** Validação em produção (smoke + escala por lag) e **destruição** (custo zero quando parado).

## 📈 Critérios de Sucesso (Definition of Done)

1. **Resiliência:** O orquestrador pode ser reiniciado no meio de uma saga e ela deve continuar de onde parou.
2. **Confiabilidade:** Nenhuma mensagem é perdida; falhas definitivas estão visíveis na DLQ.
3. **Visibilidade:** É possível abrir o Jaeger e ver exatamente quanto tempo cada etapa da saga levou.
4. **Qualidade:** Qualquer alteração no código é validada por uma suíte de testes automatizados.
5. **Performance:** O sistema processa múltiplas sagas em paralelo sem condições de corrida (race conditions).
