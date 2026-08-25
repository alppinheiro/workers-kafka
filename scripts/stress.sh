#!/bin/bash
# stress120k.sh — teste de carga máxima no ambiente local com monitoramento contínuo.
# Publica N pedidos (default 120.000 ≈ 2000/s por 1 min, se o broker aguentar) e
# amostra a cada ~15-20s: lag por consumer group, outbox pendente, sagas, throughput.
set -uo pipefail
cd /Users/andre.luiz2/Developer/workers-kafka
PREFIX="st$(date +%s | tail -c 5)"
COUNT="${1:-120000}"

PG=workers-kafka-postgres
PGR=workers-kafka-postgres-read
KAFKA=workers-kafka-broker

lag() {
  docker exec "$KAFKA" /bin/sh -c "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe 2>/dev/null | awk 'NR>2 && \$6!=\"-\" {sum[\$1]+=\$6} END {for(g in sum) printf \"%s=%d \", g, sum[g]}'" 2>/dev/null || echo "?"
}
rate() {
  curl -s --max-time 5 'http://localhost:9090/api/v1/query' --data-urlencode 'query=sum(rate(saga_events_processed_total[60s]))' 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); r=d.get("data",{}).get("result",[]); print(round(float(r[0]["value"][1]),1) if d.get("status")=="success" and r else "?")' 2>/dev/null || echo "?"
}

echo "== [$(date +%H:%M:%S)] resetando bancos =="
docker exec "$PG" psql -U saga -d saga -c "TRUNCATE sagas, saga_events, outbox RESTART IDENTITY CASCADE;" >/dev/null
docker exec "$PGR" psql -U saga -d saga_read -c "TRUNCATE order_views, processed_events RESTART IDENTITY CASCADE;" >/dev/null

echo "== [$(date +%H:%M:%S)] resetando consumer groups =="
docker exec "$KAFKA" /bin/sh -c 'for g in orchestrator worker-payment worker-inventory worker-notification order-status projector; do /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $g --reset-offsets --to-latest --execute >/dev/null 2>&1 || true; done' || true

echo "== [$(date +%H:%M:%S)] publicando $COUNT pedidos (prefixo $PREFIX) em background =="
KAFKA_BROKERS=localhost:9094 nohup go run ./cmd/load-generator -count "$COUNT" -batch 500 -prefix "$PREFIX" > /tmp/loadgen_stress.log 2>&1 &
LG_PID=$!

echo "=== FASE 1: durante a publicação ==="
echo "ts | sagas_total | outbox_pend | lag | proc_rate/s"
while kill -0 $LG_PID 2>/dev/null; do
  ts=$(date +%H:%M:%S)
  st=$(docker exec "$PG" psql -U saga -d saga -tAc "SELECT count(*) FROM sagas;" 2>/dev/null || echo "?")
  ob=$(docker exec "$PG" psql -U saga -d saga -tAc "SELECT count(*) FROM outbox WHERE published_at IS NULL;" 2>/dev/null || echo "?")
  echo "$ts | $st | $ob | $(lag) | proc=$(rate)/s"
  sleep 15
done

wait $LG_PID 2>/dev/null
echo "== [$(date +%H:%M:%S)] load-generator terminou =="
grep -a 'load-generator finalizado' /tmp/loadgen_stress.log | tail -1 || true

echo "=== FASE 2: drenagem (lag -> COMPLETED) até 6 min ==="
END_MONITOR=$(( $(date +%s) + 360 ))
while [ $(date +%s) -lt $END_MONITOR ]; do
  ts=$(date +%H:%M:%S)
  st=$(docker exec "$PG" psql -U saga -d saga -tAc "SELECT count(*) FROM sagas;" 2>/dev/null || echo "?")
  ob=$(docker exec "$PG" psql -U saga -d saga -tAc "SELECT count(*) FROM outbox WHERE published_at IS NULL;" 2>/dev/null || echo "?")
  comp=$(docker exec "$PG" psql -U saga -d saga -tAc "SELECT count(*) FROM sagas WHERE current_status='COMPLETED';" 2>/dev/null || echo "?")
  fail=$(docker exec "$PG" psql -U saga -d saga -tAc "SELECT count(*) FROM sagas WHERE current_status='FAILED';" 2>/dev/null || echo "?")
  echo "$ts | sagas=$st | outbox=$ob | COMPLETED=$comp | FAILED=$fail | lag: $(lag) | proc=$(rate)/s"
  sleep 20
done

echo "== [$(date +%H:%M:%S)] relatório final =="
docker exec "$PG" psql -U saga -d saga -c "SELECT current_status, count(*) FROM sagas GROUP BY 1 ORDER BY 2 DESC;" 2>/dev/null
echo "== FIM =="
