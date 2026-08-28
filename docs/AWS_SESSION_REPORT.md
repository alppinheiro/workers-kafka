# Relatório — Primeira Infraestrutura na AWS (Fase 10, deploy real)

> **Data:** 28/08/2026
> **Conta AWS:** `752796320852` (free tier)
> **Perfil CLI:** `lab-pessoal`
> **Resultado geral:** ✅ Infraestrutura criada, validada com teste de sucesso e **destruída por completo**.

---

## 1. Resumo executivo

Executamos o ciclo completo da Fase 10 na AWS: **Budget → Terraform → Infraestrutura → Teste → Destruição**, seguindo rigorosamente a ordem solicitada e sempre buscando o **menor custo possível** (estratégia "criar → estudar → destruir").

**Custo total estimado da sessão: < US$ 1,00** (recursos ficaram ligados por ~2h no total).

---

## 2. Ordem de execução

### 2.1 Budget (alerta de custo) — ✅ feito
| Item | Valor |
|---|---|
| Nome | `order-saga-monthly` |
| Limite | **US$ 10,00 / mês** |
| Tipo | `COST` (monitora custo real) |
| Período | `MONTHLY` |
| Alerta | 80% do limite (≈ US$ 8) por email |
| Email de notificação | `alppinheiro.aws@gmail.com` |
| Custo do budget | **Gratuito** (monitoramento + notificações não têm custo) |

### 2.2 Terraform — ✅ feito
| Etapa | Resultado |
|---|---|
| `terraform init` | ✅ OK |
| `terraform validate` | ✅ Configuração válida |
| `terraform plan` | ✅ 57 recursos a criar |
| `terraform apply` | ✅ Sucesso (após 2 correções, ver §4) |
| `terraform destroy` | ✅ `Destroy complete! Resources: 57 destroyed.` |

---

## 3. O que foi criado (e validado)

| Recurso | Configuração | Status validado |
|---|---|---|
| **VPC** | 2 AZs, subnets privadas/públicas, 1 NAT (single) | ✅ criado e destruído |
| **EKS** | `order-saga`, Kubernetes v1.31, endpoint público | ✅ **ACTIVE** |
| **Node Group** | `t3.small` **SPOT** (1–3 nós) | ✅ **Ready** |
| **RDS PostgreSQL** | `order-saga-db`, `db.t4g.micro` (free tier), PG 16.4, 20GB | ✅ **available** |
| **Pods de sistema** | `aws-node`, `coredns` (×2), `kube-proxy` | ✅ todos **Running** |

### 3.1 Teste de caso de sucesso — ✅ passou

Executado um pod `pg-test` no cluster EKS conectando ao RDS:

```sql
-- 1. Conexão EKS → RDS
SELECT version();
-- PostgreSQL 16.4 on aarch64-unknown-linux-gnu  ✅

-- 2. Criação de tabela
CREATE TABLE smoke_test (...);          -- CREATE TABLE  ✅

-- 3. Inserção
INSERT INTO smoke_test (order_id, status)
VALUES ('smoke-001','COMPLETED'),('smoke-002','FAILED');   -- INSERT 0 2  ✅

-- 4. Consulta
SELECT * FROM smoke_test;
-- id | order_id  |  status   |          created_at
--  1 | smoke-001 | COMPLETED | 2026-08-28 16:23:50+00   ✅
--  2 | smoke-002 | FAILED    | 2026-08-28 16:23:50+00   ✅
```

O fluxo **Kubernetes → rede privada → RDS → PostgreSQL → escrita/leitura** foi validado de ponta a ponta.

---

## 4. Correções necessárias durante o deploy real

O primeiro `apply` falhou por dois motivos que **só aparecem em conta real** (não no kind local):

### 4.1 Erro 1 — AMI do EKS 1.30 descontinuada
```
InvalidParameterException: Requested AMI for this version 1.30 is not supported
```
**Correção:** `cluster_version` 1.30 → **1.31** (1.30 não tem mais AMI otimizada na região).

### 4.2 Erro 2 — t3.medium não elegível para Free Tier
```
InvalidParameterCombination - The specified instance type is not eligible for Free Tier
```
**Correção:** node `t3.medium` → **`t3.small`** (elegível para free tier → custo **zero**).

### 4.3 Ajuste pós-criação — acesso IAM ao cluster
O IAM user root não tinha acesso ao EKS (`401 Unauthorized`). **Correção:** criado *Access Entry* no EKS com política `AmazonEKSClusterAdminPolicy`.

> ✅ **As correções 4.1 e 4.2 foram commitadas** no commit `19035ed`.

---

## 5. Verificação de destruição completa

| Recurso | Quantidade encontrada | Status |
|---|---|---|
| Instâncias EC2 | 1 (estado `terminated`) | ✅ nada ativo |
| Clusters EKS (todas as 17 regiões) | 0 | ✅ |
| Instâncias RDS | 0 | ✅ |
| VPCs (`order-saga*`) | 0 | ✅ |
| NAT Gateways | 1 (estado `deleted`) | ✅ |
| EIPs | 0 | ✅ |
| CloudWatch Log Groups (eks) | 0 | ✅ |
| IAM Roles (`order-saga*`) | 0 | ✅ |
| Security Groups (`order-saga*`) | 0 | ✅ |
| **Estado Terraform** | **0 recursos** | ✅ |

### 5.1 Auditoria extra (todos os serviços que geram custo)

| Serviço | Resultado | Status |
|---|---|---|
| EC2 (17 regiões) | 0 ativas | ✅ |
| RDS (16 regiões) | 0 | ✅ |
| ECS Clusters | `[]` | ✅ |
| ElastiCache | 0 | ✅ |
| OpenSearch | `[]` | ✅ |
| DocumentDB | 0 | ✅ |
| CloudFormation stacks | `[]` | ✅ |
| Load Balancers (ELBv2) | 0 | ✅ |
| EBS volumes in-use | 0 | ✅ |
| RDS Snapshots | 0 | ✅ |
| AMIs próprias | 0 | ✅ |
| CloudWatch Log Groups | `[]` | ✅ |
| **KMS Keys** | 2 (ver abaixo) | ✅ |

**KMS Keys (detalhe):**
1. `1f97a31d` — *"Default key that protects my RDS database volumes"* → **gerenciada pela AWS**, presente em toda conta, **gratuita**.
2. `262a8a64` — *"order-saga cluster encryption key"* → criada pelo EKS, já em **`PendingDeletion`** (exclusão automática em 7 dias, sem custo).

**Conclusão: infraestrutura destruída 100%, sem recursos órfãos ou cobranças residuais.**


---

## 6. Segurança e pendências

- ✅ `terraform.tfvars` (contém senha do RDS) está **protegido pelo `.gitignore`** e **não foi commitado**.
- ⚠️ A senha `kLavWTe34iU5RW7EfKDs` foi **usada no RDS criado**. Como o RDS foi destruído, não há risco residual, mas **recomenda-se gerar nova senha** no próximo `apply` (o `terraform.tfvars` local ainda contém a antiga).
- ⏳ O Budget de US$ 10/mês **continua ativo** (custo zero) — útil para monitorar futuras sessões.

---

## 7. Como repetir

```bash
# 1. Configurar credenciais
aws configure --profile lab-pessoal

# 2. Preparar infra (da raiz do projeto)
cd terraform
cp terraform.tfvars.example terraform.tfvars   # gerar NOVA senha para db_password
cd ..

# 3. Aplicar e destruir
make aws-up      # cria VPC + EKS + RDS (~15-20 min)
make aws-down    # destrói tudo (custo ≈ zero)
```

---

## 8. Próximos passos (quando voltar do almoço)

1. **Deploy do Helm chart** no EKS (se desejar subir de novo): `deploy/helm/order-saga` + KEDA + kube-prometheus-stack.
2. **ArgoCD (GitOps)**: instalar no cluster e aplicar `deploy/argocd/app.yaml` (etapa 10.4).
3. **Smoke test e2e da aplicação**: `scripts/k8s-smoke.sh` contra o cluster.
4. **Escala por lag**: validar KEDA 1→3 réplicas no EKS (como no kind).

---

# ANEXO 2 — Deploy Automático com ArgoCD (GitOps) na AWS

> **Data:** 28/08/2026 (segunda sessão)
> **Resultado:** ✅ ArgoCD instalado, aplicação deployada via GitOps e **fluxo de sucesso validado (COMPLETED)**

## O que foi feito

1. **Infra** (`make aws-up`): VPC + EKS (4 nodes t3.small SPOT) + RDS free tier.
2. **ArgoCD instalado** via Helm (`argocd/argo-cd`) no namespace `argocd`.
3. **Repositório GitHub registrado** no ArgoCD (público — sem token).
4. **Application `order-saga` criada** apontando para `deploy/helm/order-saga` + `values-prod.yaml`, com `auto-sync` + `prune` + `selfHeal`.
5. **Ajustes para produção EKS** (via Git, sincronizados automaticamente pelo ArgoCD):
   - `KAFKA_BROKERS` → `kafka.order-saga.svc.cluster.local:9092` (Kafka KRaft no cluster)
   - Kafka adicionado ao Helm chart (`templates/kafka.yaml`)
   - Migrations SQL adicionadas via ConfigMaps (`templates/migrations-configmap.yaml` usando `.Files.Glob`)
   - Secret do RDS **removido do git** (criado via `kubectl` — prática GitOps segura)

## O momento GitOps (o "deploy automático")

Ao fazer `git push` do commit que mudava o `KAFKA_BROKERS`, o ArgoCD **detectou e sincronizou sozinho**:

```
Sync Status: Synced to main (0294204)   ← sem nenhum comando manual
```

E a cada ajuste seguinte (`124e3af` Kafka, `22c9f99` migrations), o ArgoCD aplicou automaticamente.

## Validação de ponta a ponta

| Etapa | Resultado |
|---|---|
| 9 deployments + Kafka + KEDA + ArgoCD | ✅ Todos Running |
| Migrations no RDS | ✅ `sagas`, `saga_events`, `outbox` criados |
| Pedido `e2e-argocd-002` | ✅ Saga processada (FAILED — falha controlada do simulador) |
| Pedido `e2e-ok-004` | ✅ **COMPLETED** — saga completa com sucesso |
| RDS final | `1 COMPLETED` + `1 FAILED` |

## Pendências técnicas resolvidas no caminho

- **EBS CSI driver**: instalado com IAM role dedicada (IRSA) — sem ele o PVC do Kafka não provisiona.
- **StorageClass `gp2`**: marcada como default (PVC do Kafka precisava).
- **Node scaling**: 1→2→3→4 nodes (limite de 11 pods/node no t3.small; ArgoCD+KEDA+aplicação precisam de ~35 slots).
- **Banco `saga_read`**: criado manualmente (RDS cria só o `saga` por padrão).
- **Secret do RDS**: gerenciado fora do ArgoCD (removido do Helm) para não ser sobrescrito.

## Como repetir (próxima sessão)

```bash
make aws-up                              # infra
# depois: instalar ArgoCD + KEDA + Kafka + aplicar secrets (ver roteiro PHASE_10 §10.4)
git push origin main                     # ArgoCD sincroniza sozinho
```

