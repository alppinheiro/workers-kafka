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
| Clusters EKS | 0 | ✅ |
| Instâncias RDS | 0 | ✅ |
| VPCs (`order-saga*`) | 0 | ✅ |
| NAT Gateways | 1 (estado `deleted`) | ✅ |
| EIPs | 0 | ✅ |
| CloudWatch Log Groups (eks) | 0 | ✅ |
| IAM Roles (`order-saga*`) | 0 | ✅ |
| Security Groups (`order-saga*`) | 0 | ✅ |
| **Estado Terraform** | **0 recursos** | ✅ |

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
