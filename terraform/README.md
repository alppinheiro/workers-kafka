# Terraform — Fase 10 (Cloud AWS: VPC + EKS + RDS)

Infraestrutura **sem custo de execução local** (o custo só existe enquanto o
`terraform apply` estiver ativo). Provisiona:

- **VPC** (2 AZs, subnets privadas/públicas, 1 NAT — módulo `terraform-aws-modules/vpc`).
- **EKS** (control plane gerenciado + node group `t3.medium` **SPOT** — módulo `terraform-aws-modules/eks`).
- **RDS PostgreSQL** `db.t4g.micro` (free tier 12 meses) na sub-rede privada, acessível
  apenas pelos nodes do EKS.

> Kafka fica **dentro do cluster** (Strimzi ou `apache/kafka`, como na Fase 9) para reduzir
> custo; MSK é alternativa documentada no `PHASE_10_PLAN.md` (§5).

## Pré-requisitos

```bash
brew install terraform awscli
aws configure   # credenciais (access key + secret)
```

## Uso

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # ajuste a senha do RDS
terraform init
terraform plan
terraform apply -auto-approve   # ~15-20 min (EKS)
terraform output kubeconfig     # → aws eks update-kubeconfig ...
# 1x: criar o database de leitura (RDS cria só o "saga" no launch)
#   kubectl run pg --image=postgres:16-alpine --rm -it --restart=Never -- \
#     psql "$(kubectl get secret ... )" -c 'CREATE DATABASE saga_read'
# depois: deploy do Helm + ArgoCD (ver PHASE_10_PLAN §10.3/§10.4)
```

## Destruição (custo ≈ zero quando parado)

```bash
cd terraform && terraform destroy -auto-approve
```

> ⚠️ `terraform destroy` remove RDS (sem snapshot final) e o EKS. O `terraform.tfvars`
> com a senha **não deve ir para o git** (adicionar `terraform.tfvars` ao `.gitignore`).
