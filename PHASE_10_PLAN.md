# Fase 10: Cloud AWS (EKS + Terraform + GitOps)

> Plano de execução. Contexto: Fases 1–9 concluídas (pipeline validado em kind com Helm + KEDA;
> CI publica imagens **multi-arch** no GHCR). Objetivo: subir o projeto em **produção na AWS**,
> aprendendo Terraform, EKS, serviços gerenciados e GitOps com **ArgoCD**, com **custo controlado**.

## 1. Objetivo

Deploy em **produção na AWS** de ponta a ponta, reutilizando tudo que já foi validado:

- **EKS** (Kubernetes gerenciado) provisionado com **Terraform** (IaC).
- Imagens **multi-arch** do GHCR (CI da Fase 8) — sem rebuild.
- **KEDA** escalando por lag (já validado no kind, Fase 9).
- **ArgoCD (GitOps)**: o repositório é a fonte da verdade do deploy.
- Kafka e Postgres **gerenciados** (MSK/RDS) ou **self-hosted no EKS** — decisão de custo.
- **Observabilidade**: kube-prometheus-stack + dashboard "Saga - Visão Geral" (pendência 9.7).
- **Custo controlado**: infra 100% IaC → `terraform destroy` quando não estiver estudando
  (EKS cobra ~US$ 0,10/h de control plane + nodes + serviços usados).

## 2. Arquitetura (AWS)

```
Internet ──► ALB/Ingress (nginx) ──► Grafana / APIs
AWS ─────────────────────────────────────────────
 ├── VPC (2 AZs, subnets pública/privada)
 ├── EKS (control plane gerenciado) + NodeGroup (managed, 1–2 t3.medium/spot)
 ├── RDS PostgreSQL (db.t4g.micro — free tier)  [ou Postgres no EKS]
 ├── Kafka: MSK (gerenciado) OU Strimzi no EKS (custo)  [decisão §5]
 ├── ECR: não usado (imagens vêm do GHCR)
 └── IAM/IRSA: ServiceAccounts p/ RDS/MSK/pull
Kubernetes (namespace order-saga):
 ├── app: Helm chart `deploy/helm/order-saga` (8 Deployments + migrations Job + ScaledObjects)
 ├── KEDA (ScaledObject por consumer lag)
 ├── ArgoCD (Application → chart do repo, auto-sync)
 └── kube-prometheus-stack (Prometheus + Grafana + kube-state-metrics)
```

## 3. Etapas

### 10.1 — Terraform base (VPC + EKS)
- `terraform/` no repo: providers AWS, módulos VPC + EKS (ou `terraform-aws-modules/eks`).
- NodeGroup managed (1–2 instâncias `t3.medium`, **spot** para reduzir custo).
- IRSA: OIDC do EKS + IAM roles para os ServiceAccounts (pull GHCR, acesso RDS/MSK).
- Outputs: `kubeconfig`, endpoint, node role ARN.

### 10.2 — Infra gerenciada (RDS + Kafka)
- **RDS PostgreSQL** `db.t4g.micro` (free tier 12 meses, single-AZ) + subnet group + security group.
  (Fallback de custo: Postgres como Deployment+PVC no EKS, como no kind.)
- **Kafka**: duas opções documentadas —
  - **MSK** (gerenciado, multi-AZ): experiência "empresa", custo alto (~US$ 0,5+/h broker).
  - **Strimzi no EKS** (self-hosted): experiência de operador/CRD, custo do node apenas.
  - **Decisão recomendada p/ estudo: Strimzi no EKS** (ou apache/kafka, como no kind) — MSK
    somente se o custo não for um problema.
- Tópicos: via CRD (Strimzi) ou Job `kafka-init` (apache/kafka) — 4 partições, como validado.

### 10.3 — Deploy do Helm chart no EKS
- Reutilizar `deploy/helm/order-saga` com `values-prod.yaml` (brokers MSK/Strimzi, RDS endpoint,
  secrets via `ExternalSecret`/SealedSecrets ou Secret simples — estudo).
- KEDA instalado; `ScaledObject` idênticos ao kind (lag 200, min 1, max 3).
- **Ingress nginx** + `kubectl port-forward`/ALB para Grafana (sem TLS — escopo do estudo).

### 10.4 — GitOps com ArgoCD
- Instalar ArgoCD (`helm install argo-cd argo/argo-cd`).
- `Application` apontando para `https://github.com/alppinheiro/workers-kafka` →
  path `deploy/helm/order-saga` (ou `deploy/argocd/app.yaml`), targetRevision `main`, auto-sync.
- Fluxo: push no git → ArgoCD sincroniza (imagens novas via tag sha).

### 10.5 — Observabilidade no cluster
- `kube-prometheus-stack` (Prometheus + Grafana + kube-state-metrics + node-exporter) — fecha a 9.7.
- Importar o dashboard "Saga - Visão Geral" (grafana/dashboards) + alertas
  (`SagaDLQGrowth`, `SagaConsumerStalled`).

### 10.6 — Validação e destruição
- Smoke no EKS: criar pedido → saga `COMPLETED` (scripts/k8s-smoke.sh apontando p/ EKS).
- Escala por lag: load-generator no cluster → KEDA 1→3 (mesma validação do kind).
- **Destruição**: `terraform destroy` + `make` docs do procedimento → custo ≈ zero quando parado.

## 4. Custos (referência, 24/08/2026)

| Recurso | Custo | Observação |
|---|---|---|
| EKS control plane | ~US$ 0,10/h | por hora ativo; **não tem free tier** |
| Node t3.medium (1–2, spot) | ~US$ 0,01–0,02/h | spot reduz bastante |
| RDS db.t4g.micro | free tier 12 meses | senão ~US$ 15–25/mês |
| MSK (broker mínimo) | ~US$ 0,5+/h | **caro** — recomendado Strimzi no EKS |
| ECR | US$ 0 | imagens no GHCR |

**Estratégia:** sessões de estudo de 2–4 h com `terraform apply`/`destroy` ≈ **US$ 1–3 por sessão**
(control plane + node). Nunca deixar ligado 24/7.

## 5. Decisões de arquitetura

| Tema | Recomendação | Alternativa |
|---|---|---|
| Provisionamento | **Terraform** (módulos VPC + EKS) | eksctl (mais simples, menos IaC) |
| Kafka | **Strimzi no EKS** (custo/experiência) | MSK (gerenciado, caro) |
| Postgres | **RDS free tier** | Postgres no EKS (custo zero) |
| Deploy | **ArgoCD (GitOps)** + Helm chart existente | helm upgrade manual (CI/CD) |
| Autoscaling | **KEDA** por lag (igual kind) | — |
| Observabilidade | kube-prometheus-stack + dashboard existente | CloudWatch (pago) |
| Segurança | Secrets simples/SealedSecrets (estudo) | External Secrets/SSM |

## 6. Ordem de execução e critérios de pronto

| Etapa | Risco | Dependência | DoD |
|---|---|---|---|
| 10.1 Terraform (VPC+EKS) | médio (credenciais AWS) | conta AWS + `awscli` | `terraform apply` cria cluster; `kubectl get nodes` Ready |
| 10.2 Infra (RDS + Kafka) | médio (custo/segurança de grupos) | 10.1 | RDS acessível; Kafka com tópicos 4 partições |
| 10.3 Helm no EKS | médio (endpoints/credenciais) | 10.2 | Deployments Ready; smoke `k8s-smoke` verde no EKS |
| 10.4 ArgoCD | médio (permissões Git) | 10.3 | `Application` synced; mudança de tag auto-sincroniza |
| 10.5 Observabilidade | baixo | 10.3 | Grafana com dashboard "Saga - Visão Geral" + alertas |
| 10.6 Validação + destroy | baixo | 10.3–10.5 | KEDA 1→3 no EKS; `terraform destroy` documentado e testado |

## 7. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Custo inesperado (EKS/MSK ligado 24/7) | `terraform destroy` no fim da sessão; alerta de billing; evitar MSK |
| Credenciais AWS/segredos vazando | `aws configure` com perfil local; secrets só no cluster; nada no git |
| IRSA/ServiceAccounts mal configurado | validar com pod de teste; IAM mínimo (least privilege) |
| EKS demora ~15–20 min para criar | usar módulo `terraform-aws-modules/eks`; `terraform apply` em background |
| GHCR privado (pull no EKS) | tornar pacotes públicos OU `imagePullSecret` (dockerconfigjson) |
| Endpoints RDS/MSK fora do VPC | security groups + subnets privadas; `kubectl port-forward` p/ debug |

## 8. Fora de escopo desta fase

- TLS/HTTPS e cert-manager (estudo — sem "produção bonitinha" de segurança).
- Multi-região / DR / backups avançados (RDS faz snapshot básico).
- Service mesh (Istio/Linkerd), multicluster.
- Alta disponibilidade do Kafka/Postgres em nível de empresa (foco: experiência + custo).

## 9. Entregáveis desta fase

1. `terraform/` — VPC + EKS + RDS (módulos), variáveis por ambiente, `terraform apply/destroy`.
2. `deploy/helm/order-saga/values-prod.yaml` — endpoints cloud (RDS/MSK/Strimzi) + secrets.
3. `deploy/argocd/app.yaml` — Application ArgoCD (GitOps, auto-sync).
4. KEDA no EKS + kube-prometheus-stack + dashboard "Saga - Visão Geral" importado.
5. `scripts/k8s-smoke.sh` rodando contra o EKS + validação de escala por lag.
6. Documentação: procedimento de custo (criar/destruir) no README/runbook.

## 10. Andamento (24/08/2026) — parte sem custo concluída

Preparados **sem custo** (aplicação aguarda credenciais AWS):

- ✅ `terraform/` — `versions.tf`, `variables.tf`, `main.tf` (VPC + EKS + RDS), `outputs.tf`,
  `terraform.tfvars.example` e `README.md`. `terraform init` + `terraform validate` **OK**
  (módulos `terraform-aws-modules/vpc` e `eks` ~>20; RDS `db.t4g.micro` free tier).
- ✅ `deploy/helm/order-saga/values-prod.yaml` — endpoints de produção (RDS/Strimzi) e notas
  de segredos; render Helm validado.
- ✅ `deploy/argocd/app.yaml` — Application ArgoCD (GitOps, auto-sync, `values-prod.yaml`).
- ✅ `Makefile`: `make aws-up` / `make aws-down` (terraform apply/destroy).
- ✅ `.gitignore` protege `terraform.tfvars`/state.

**Pendente (com custo / credenciais):** `aws configure` + `make aws-up` → aplicar o chart +
KEDA + ArgoCD + kube-prometheus-stack (10.3–10.6).

