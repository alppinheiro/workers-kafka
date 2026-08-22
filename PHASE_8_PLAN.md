# Fase 8: CI/CD com GitHub Actions

> Plano de execução. Contexto: `EVOLUTION_PLAN.md` (roadmap) e validação da Fase 7
> (benchmark A/B/C + resiliência R1–R4 + runbook). Objetivo: rede de segurança
> automática antes do deploy em Kubernetes (Fase 9/10).

## Objetivo

Garantir que **toda mudança no código** seja validada automaticamente e que as imagens
Docker sejam **geradas e publicadas** de forma reprodutível. É o pré-requisito para
qualquer deploy em produção (Helm/ArgoCD na Fase 9).

Hoje a validação é 100% manual (`make check` + `make integration` + smoke local). O CI
vai executar exatamente esses mesmos passos no GitHub, sem depender da máquina local.

## 1. Pipeline — 4 jobs

```
push na main / pull_request
        │
        ▼
┌──────────────┐   ┌───────────────┐   ┌──────────────┐   ┌───────────────┐
│ 1. check     │   │ 2. integration│   │ 3. smoke     │   │ 4. build-images│
│ quality +    │   │ Testcontainers│   │ e2e compose  │   │ GHCR (9 svcs)  │
│ unit tests   │   │ Kafka+Postgres│   │ saga→COMPL.  │   │ :sha e :latest │
└──────────────┘   └───────────────┘   └──────────────┘   └───────────────┘
      │                   │                  │                    │
      └──────────────┬────┴────────┬─────────┴────────────────────┘
                     ▼             ▼
              checks verdes   (main) imagens publicadas
```

### Job 1 — `check` (qualidade + unitários)
- Runner: `ubuntu-latest`.
- Passos: `gofmt -l .`, `go vet ./...`, `go build ./...`, `go test ./... -race -cover`
  e `golangci-lint run ./...` (ação oficial `golangci/golangci-lint-action`).
- **Cache** do Go (`actions/setup-go` com `cache: true`): módulos + build cache.
- Critério: equivalente ao `make check` local, sem `DATABASE_URL` (testes de integração
  são pulados por design).

### Job 2 — `integration` (Testcontainers — Kafka + Postgres reais)
- Roda os testes com build tag `integration`:
  `go test -tags integration ./internal/infrastructure/kafka/... ./internal/infrastructure/persistence/postgres/...`
- Requer **Docker** — `ubuntu-latest` já tem.
- ⚠️ **Ajuste necessário**: o `make integration` (Makefile) força
  `DOCKER_HOST=unix://$HOME/.colima/...` (só existe local). No CI, o Docker nativo já
  está configurado → chamar `go test -tags integration ...` diretamente (ou parametrizar
  o Makefile para não sobrescrever `DOCKER_HOST` quando já definido).
- Valida: round-trip Kafka, fluxo completo da saga com Postgres real, e
  `TestUnitOfWork_AtomicCommit/Rollback`.

### Job 3 — `smoke` (e2e com docker-compose — o "teste de produção")
- Sobe a stack com o **mesmo `docker-compose.yml`** do projeto (sem `.env` — defaults).
- Aguarda healthchecks (kafka + postgres `healthy`).
- `make create-order ORDER_ID=ci-smoke-<sha>` e, após ~25 s, consulta no container
  `workers-kafka-postgres`:
  - `sagas`: `current_status IN ('COMPLETED','FAILED')`
  - `saga_events`: journal populado
  - `outbox`: `published_at IS NOT NULL` (outbox drenada)
  - read model (`order_views`) consistente
- Garante que **imagem → container → pipeline → banco** funcionam de ponta a ponta.

### Job 4 — `build-images` (build + push GHCR)
- `docker/build-push-action` para os **9 serviços** (o Dockerfile usa `TARGET`):
  `orchestrator, worker-payment, worker-inventory, worker-notification, outbox-relay,
  projector, order-status, metrics-exporter, create-order`.
- Imagens: `ghcr.io/<owner>/workers-kafka-<svc>`
  - push na `main` → `:sha-<sha>` + `:latest`
  - tag `v*` → `:vX.Y.Z`
- Login: `GITHUB_TOKEN` com `permissions: packages: write, contents: read`.
- **Cache de camadas** do buildx (`type=gha`) para builds rápidos.

## 2. Gatilhos

| Evento | Jobs |
|---|---|
| `push` na `main` | check + integration + smoke + build-images |
| `pull_request` | check + integration + smoke (sem push de imagem) |
| `tag v*` | check + integration + build-images (tag semver) |
| `workflow_dispatch` | manual (qualquer branch) |

## 3. Configuração do repositório (ação do usuário)

- **Branch protection** em `main`: PR obrigatório + aprovação; status checks exigidos
  (`check`, `integration`, `smoke`).
- **GHCR**: o `GITHUB_TOKEN` já é suficiente (sem segredo manual). Tornar as imagens
  públicas (package visibility) para facilitar o pull na Fase 9/10.
- **Nada de segredo no repo**: o CI usa credenciais default do compose (saga/saga) —
  credenciais reais de produção virão de `Secret`/`ExternalSecret` no Kubernetes.

## 4. Ordem de execução e critérios de pronto

| Etapa | Risco | Dependência | DoD |
|---|---|---|---|
| 8.1 job `check` | baixo | — | `make check` verde no runner |
| 8.2 job `integration` | médio (DOCKER_HOST) | 8.1 | Testcontainers verdes no runner |
| 8.3 job `smoke` | médio (tempo) | 8.2 | saga `ci-smoke-*` chega a terminal no compose do CI |
| 8.4 job `build-images` | baixo (GHCR) | 8.3 | 9 imagens no GHCR (`:sha`/`:latest`) |
| 8.5 proteção de branch | baixo | 8.4 | PR bloqueia sem checks verdes |

## 5. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Testcontainers puxam imagens todo run | cache do Docker; `docker pull` explícito no job |
| `DOCKER_HOST` do Makefile aponta para colima (só local) | job chama `go test -tags integration` diretamente; Makefile fica como conveniência local |
| Smoke depende do compose subir em ~1 min | healthcheck de kafka/postgres + `timeout` generoso |
| Push de imagem em PR de fork | build-images só roda em `main`/`tags` (eventos confiáveis) |
| OOM/lentidão | runner ubuntu-latest tem 7 GB/2 vCPU — suficiente para Kafka+Postgres |

## 6. Fora de escopo desta fase (Fases 9/10)

- Helm chart + deploy no Kubernetes (Fase 9) — o CI **entrega** as imagens no GHCR.
- ArgoCD/GitOps e deploy automático em EKS (Fase 10) — consumirá as imagens do GHCR.
- Secrets de produção (External Secrets Operator) — só no K8s.

## 7. Entregáveis desta fase

1. `.github/workflows/ci.yml` (check + integration + smoke + build-images).
2. Ajuste do `Makefile` (integration sem sobrescrever `DOCKER_HOST` quando definido) ou
   comando CI dedicado.
3. `make ci` local — reproduz o pipeline do CI na máquina (iteração rápida antes do push).
4. Badge de status no README.
5. Branch protection configurada no repositório (ação manual do usuário).
