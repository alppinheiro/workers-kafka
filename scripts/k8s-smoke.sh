#!/bin/bash
# k8s-smoke.sh <order_id> — smoke end-to-end no cluster (Fase 9):
# cria um pedido (Job/pod create-order), aguarda e valida saga terminal +
# journal + outbox drenada + read model. Requer cluster kind de pé.
set -euo pipefail
NS="${K8S_NAMESPACE:-order-saga}"
ORDER_ID="${1:-k8s-smoke}"
IMG_TAG="${K8S_IMG_TAG:-latest}"

echo "== [$(date +%H:%M:%S)] criando pedido $ORDER_ID =="
kubectl run create-order-"$ORDER_ID" \
  --image="ghcr.io/alppinheiro/workers-kafka-create-order:$IMG_TAG" \
  --env="KAFKA_BROKERS=kafka:9092" \
  --env="OTEL_EXPORTER_OTLP_ENDPOINT=order-saga-otel:4318" \
  -n "$NS" --restart=Never -- "$ORDER_ID"

kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/create-order-"$ORDER_ID" \
  -n "$NS" --timeout=120s

echo "== [$(date +%H:%M:%S)] aguardando o pipeline (30s) =="
sleep 30

pg()  { kubectl exec deploy/order-saga-postgres -n "$NS" -- psql -U saga -d saga -tAc "$1"; }
pgr() { kubectl exec deploy/order-saga-postgres-read -n "$NS" -- psql -U saga -d saga_read -tAc "$1"; }

echo "== [$(date +%H:%M:%S)] validações =="
saga_status=$(pg "SELECT current_status FROM sagas WHERE order_id='$ORDER_ID'")
echo "sagas: $saga_status"
test "$saga_status" = "COMPLETED" -o "$saga_status" = "FAILED"

journal_rows=$(pg "SELECT count(*) FROM saga_events WHERE order_id='$ORDER_ID'")
echo "saga_events: $journal_rows"
test "$journal_rows" -gt 0

pend=1
for _ in $(seq 1 12); do
  pend=$(pg "SELECT count(*) FROM outbox WHERE key='$ORDER_ID' AND published_at IS NULL")
  [ "$pend" -eq 0 ] && break
  sleep 5
done
echo "outbox pendente: $pend"
test "$pend" -eq 0

rm_status=""
for _ in $(seq 1 12); do
  rm_status=$(pgr "SELECT current_status FROM order_views WHERE order_id='$ORDER_ID'")
  [ "$rm_status" = "COMPLETED" -o "$rm_status" = "FAILED" ] && break
  sleep 5
done
echo "order_views: $rm_status"
test "$rm_status" = "COMPLETED" -o "$rm_status" = "FAILED"

echo "=== K8S SMOKE OK ==="
