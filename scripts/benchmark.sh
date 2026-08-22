#!/bin/bash
# benchmark.sh <prefix> <count> — cenário do benchmark da Etapa 7.5 (validação final).
#
# Reseta bancos, reseta consumer groups, publica N pedidos, aguarda janela fixa e
# reporta o estado das sagas + outbox pendente + throughput (Prometheus).
#
# Uso:
#   scripts/benchmark.sh bA 3000     # Cenário A (1 réplica, SAGA_WORKERS=1, 1 relay)
#   scripts/benchmark.sh bB 3000     # Cenário B (1 réplica, SAGA_WORKERS=3, 1 relay)
#   scripts/benchmark.sh bC 3000     # Cenário C (3 réplicas, SAGA_WORKERS=1, 2 relays)
#
# Pré-requisito: stack de pé (make up) com os serviços do pipeline recriados.
set -euo pipefail
cd "$(dirname "$0")/.."
PREFIX="${1:-bA}"
COUNT="${2:-3000}"

PG=workers-kafka-postgres
PGR=workers-kafka-postgres-read
KAFKA=workers-kafka-broker

echo "== [$(date +%H:%M:%S)] resetando bancos =="
docker exec "$PG" psql -U saga -d saga -c "TRUNCATE sagas, saga_events, outbox RESTART IDENTITY CASCADE;" >/dev/null
docker exec "$PGR" psql -U saga -d saga_read -c "TRUNCATE order_views, processed_events RESTART IDENTITY CASCADE;" >/dev/null

echo "== [$(date +%H:%M:%S)] resetando consumer groups =="
docker exec "$KAFKA" /bin/sh -c 'for g in orchestrator worker-payment worker-inventory worker-notification order-status projector; do /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $g --reset-offsets --to-latest --execute >/dev/null 2>&1 || true; done' || true

echo "== [$(date +%H:%M:%S)] publicando $COUNT pedidos (prefixo $PREFIX) =="
KAFKA_BROKERS=localhost:9094 go run ./cmd/load-generator -count "$COUNT" -batch 500 -prefix "$PREFIX"

echo "== [$(date +%H:%M:%S)] aguardando 80s =="
sleep 80

echo "== [$(date +%H:%M:%S)] estado das sagas =="
docker exec "$PG" psql -U saga -d saga -c "SELECT current_status, count(*) FROM sagas WHERE order_id LIKE '$PREFIX%' GROUP BY 1 ORDER BY 2 DESC;"

echo "== [$(date +%H:%M:%S)] outbox pendente =="
docker exec "$PG" psql -U saga -d saga -c "SELECT count(*) AS outbox_pendente FROM outbox WHERE published_at IS NULL;"

echo "== [$(date +%H:%M:%S)] throughput (Prometheus) =="
curl -s --max-time 10 'http://localhost:9090/api/v1/query' --data-urlencode 'query=sum(rate(saga_events_processed_total[60s]))' | python3 -c 'import sys,json; d=json.load(sys.stdin); r=d.get("data",{}).get("result",[]); print("rate(saga_events_processed_total[60s]) =", r[0].get("value",["","n/a"])[1] if d.get("status")=="success" and r else d)'
