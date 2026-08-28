#!/bin/bash
# aws-bootstrap.sh — automatiza o pós-apply do Terraform no EKS (Fase 10).
# Roda APÓS `make aws-up` (infra criada) e antes do deploy do ArgoCD.
#
# Faz tudo que antes era manual:
#   1. kubeconfig do EKS
#   2. cria o banco saga_read no RDS (o RDS só cria o "saga")
#   3. cria o Secret order-saga-secrets (DATABASE_URL real, fora do git)
#   4. instala o ArgoCD
#   5. instala o KEDA
#   6. aplica a Application do ArgoCD (deploy GitOps)
#
# Uso:
#   AWS_PROFILE=lab-pessoal bash scripts/aws-bootstrap.sh
set -euo pipefail
cd "$(dirname "$0")/.."

REGION="${AWS_REGION:-us-east-1}"
CLUSTER="${CLUSTER_NAME:-order-saga}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
RDS_HOST="$(cd terraform && terraform output -raw rds_endpoint | sed 's/:5432//')"
RDS_PASS="$(grep 'db_password' terraform/terraform.tfvars | awk -F'"' '{print $2}')"
DB_URL="postgres://saga:${RDS_PASS}@${RDS_HOST}:5432/saga?sslmode=require"
DB_URL_READ="postgres://saga:${RDS_PASS}@${RDS_HOST}:5432/saga_read?sslmode=require"

echo "== [bootstrap] cluster=$CLUSTER rds=$RDS_HOST =="

# 1. kubeconfig
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER"

# 2. Cria o banco saga_read (o RDS cria só o "saga" no launch)
kubectl run pg-init --image=postgres:16-alpine --restart=Never -n default \
  --env="PGPASSWORD=$RDS_PASS" --command -- \
  /bin/sh -c "psql -h $RDS_HOST -U saga -d saga -c 'CREATE DATABASE saga_read;'" 2>/dev/null || true
kubectl wait --for=condition=Ready pod/pg-init --timeout=120s 2>/dev/null || true
sleep 5
kubectl logs pg-init 2>/dev/null | tail -2 || true
kubectl delete pod pg-init --force --grace-period=0 2>/dev/null || true

# 3. Secret do RDS (não versionado no git; o ArgoCD tem ignoreDifferences)
kubectl create namespace order-saga --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl create secret generic order-saga-secrets -n order-saga \
  --from-literal="DATABASE_URL=$DB_URL" \
  --from-literal="DATABASE_URL_READ=$DB_URL_READ" \
  -o yaml --dry-run=client | kubectl apply -f -

echo "== [bootstrap] ArgoCD =="
helm repo add argo https://argoproj.github.io/argo-helm 2>/dev/null || true
helm install argocd argo/argo-cd -n argocd --create-namespace || true
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=argocd-server -n argocd --timeout=180s

echo "== [bootstrap] KEDA =="
helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
helm install keda kedacore/keda --namespace keda --create-namespace || true

echo "== [bootstrap] Aplicando Application do ArgoCD =="
# Aguarda o CRD do ArgoCD existir
for i in $(seq 1 20); do
  kubectl get crd applications.argoproj.io >/dev/null 2>&1 && break
  sleep 5
done
kubectl apply -f deploy/argocd/app.yaml

echo "== [bootstrap] Pronto! Aguardando ArgoCD sincronizar =="
echo "UI:      kubectl port-forward svc/argocd-server -n argocd 8080:443"
echo "Senha:   kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
