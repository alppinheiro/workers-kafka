#!/bin/bash
# aws-cleanup.sh — remove recursos ÓRFÃOS que o `terraform destroy` NÃO cobre.
# Roda APÓS `make aws-down` para garantir custo zero.
#
# O que o Terraform NÃO remove (e este script limpa):
#   1. Volumes EBS dinâmicos (criados via StorageClass/CSI — fora do estado Terraform)
#   2. IAM roles criadas manualmente (ex: AmazonEKS_EBS_CSI_DriverRole)
#   3. Snapshots RDS órfãos
#   4. AMIs próprias (imagens não usadas)
#   5. Log groups CloudWatch do EKS
#
# Uso:
#   AWS_PROFILE=lab-pessoal bash scripts/aws-cleanup.sh
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
CLUSTER_PREFIX="${CLUSTER_PREFIX:-order-saga}"
PROFILE_ARG=""
[ -n "${AWS_PROFILE:-}" ] && PROFILE_ARG="--profile $AWS_PROFILE"

echo "== [cleanup] região=$REGION prefixo=$CLUSTER_PREFIX =="
echo "AVISO: vai DELETAR volumes EBS órfãos e recursos não gerenciados pelo Terraform."
read -r -p "Continuar? [y/N] " resp
[ "$resp" = "y" ] || { echo "Abortado."; exit 0; }

# 1. Volumes EBS órfãos (available e com nome do projeto)
echo "== [cleanup] Volumes EBS órfãos =="
IDS=$(aws ec2 describe-volumes $PROFILE_ARG --region "$REGION" \
  --filters "Name=status,Values=available" "Name=tag:Name,Values=${CLUSTER_PREFIX}*" \
  --query 'Volumes[].VolumeId' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true)
if [ -n "$IDS" ]; then
  for vol in $IDS; do
    echo "  deletando volume órfão: $vol"
    aws ec2 delete-volume $PROFILE_ARG --region "$REGION" --volume-id "$vol"
  done
else
  echo "  nenhum volume EBS órfão"
fi

# Fallback: volumes available sem filtro de tag (para capturar qualquer resíduo)
IDS=$(aws ec2 describe-volumes $PROFILE_ARG --region "$REGION" \
  --filters "Name=status,Values=available" \
  --query 'Volumes[].VolumeId' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true)
if [ -n "$IDS" ]; then
  for vol in $IDS; do
    echo "  [fallback] deletando volume available: $vol"
    aws ec2 delete-volume $PROFILE_ARG --region "$REGION" --volume-id "$vol"
  done
else
  echo "  nenhum outro volume available"
fi

# 2. IAM roles manuais do projeto (criadas fora do Terraform)
echo "== [cleanup] IAM roles manuais =="
for role in "AmazonEKS_EBS_CSI_DriverRole"; do
  if aws iam get-role $PROFILE_ARG --role-name "$role" >/dev/null 2>&1; then
    # remove policies anexadas antes de deletar
    for pol in $(aws iam list-attached-role-policies $PROFILE_ARG --role-name "$role" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
      aws iam detach-role-policy $PROFILE_ARG --role-name "$role" --policy-arn "$pol" 2>/dev/null || true
    done
    aws iam delete-role $PROFILE_ARG --role-name "$role" && echo "  role $role deletada"
  fi
done

# 3. Snapshots RDS órfãos (do cluster apagado)
echo "== [cleanup] Snapshots RDS =="
SNAP=$(aws rds describe-db-snapshots $PROFILE_ARG --region "$REGION" \
  --query "DBSnapshots[?starts_with(DBSnapshotIdentifier,'${CLUSTER_PREFIX}')].DBSnapshotIdentifier" \
  --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true)
if [ -n "$SNAP" ]; then
  for s in $SNAP; do
    echo "  deletando snapshot: $s"
    aws rds delete-db-snapshot $PROFILE_ARG --region "$REGION" --db-snapshot-identifier "$s" >/dev/null
  done
else
  echo "  nenhum snapshot do projeto"
fi

# 4. AMIs próprias não usadas
echo "== [cleanup] AMIs próprias =="
AMI=$(aws ec2 describe-images $PROFILE_ARG --region "$REGION" --owners self \
  --query 'Images[].ImageId' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true)
if [ -n "$AMI" ]; then
  for a in $AMI; do
    echo "  deregistrando AMI: $a"
    aws ec2 deregister-image $PROFILE_ARG --region "$REGION" --image-id "$a" >/dev/null 2>&1 || true
  done
else
  echo "  nenhuma AMI própria"
fi

# 5. Log groups CloudWatch do EKS
echo "== [cleanup] CloudWatch Log Groups (eks) =="
LOGS=$(aws logs describe-log-groups $PROFILE_ARG --region "$REGION" \
  --log-group-name-prefix "/aws/eks" --query 'logGroups[].logGroupName' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true)
if [ -n "$LOGS" ]; then
  for lg in $LOGS; do
    echo "  deletando log group: $lg"
    aws logs delete-log-group $PROFILE_ARG --region "$REGION" --log-group-name "$lg" >/dev/null 2>&1 || true
  done
else
  echo "  nenhum log group /aws/eks"
fi

echo "== [cleanup] VERIFICAÇÃO FINAL =="
echo "EBS volumes: $(aws ec2 describe-volumes $PROFILE_ARG --region "$REGION" --query 'Volumes[].VolumeId' --output text 2>/dev/null | tr '\t' '\n' | grep -cv '^$' || true)"
echo "EC2 ativas:  $(aws ec2 describe-instances $PROFILE_ARG --region "$REGION" --query "Reservations[].Instances[?State.Name!='terminated'].InstanceId" --output text 2>/dev/null | tr '\t' '\n' | grep -cv '^$' || true)"
echo "== [cleanup] Concluído =="
