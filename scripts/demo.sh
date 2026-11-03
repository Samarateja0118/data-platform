#!/usr/bin/env bash
# One-command demo: start the platform, stream live events, run the ELT DAG, print a report.
set -euo pipefail
cd "$(dirname "$0")/.."
STREAM_SECONDS="${STREAM_SECONDS:-180}"

echo "==> building and starting Redpanda, Postgres, consumer, producer, Airflow"
if [[ -n "${NO_BUILD:-}" ]]; then docker compose up -d --no-build; else docker compose up -d --build; fi

echo "==> waiting for Airflow to come up"
for _ in $(seq 1 60); do
  docker compose exec -T airflow airflow dags list 2>/dev/null | grep -q ridestream_elt && break
  sleep 5
done
docker compose exec -T airflow airflow dags unpause ridestream_elt >/dev/null

echo "==> streaming live ride events for ${STREAM_SECONDS}s (watch: docker compose logs -f consumer)"
sleep "$STREAM_SECONDS"

echo "==> triggering the ELT DAG"
run_id="demo_$(date +%s)"
docker compose exec -T airflow airflow dags trigger ridestream_elt -r "$run_id" >/dev/null
for _ in $(seq 1 60); do
  state=$(docker compose exec -T airflow airflow dags list-runs ridestream_elt -o plain 2>/dev/null \
          | awk -v r="$run_id" '$2==r {print $3}')
  [[ "$state" == "success" || "$state" == "failed" ]] && break
  sleep 5
done
echo "    DAG run $run_id: ${state:-unknown}"

echo "==> report"
docker compose exec -T postgres psql -U platform -d warehouse < scripts/report.sql
echo
echo "Airflow UI: http://localhost:8080 (password: docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated)"
[[ "${state:-}" == "success" ]]
