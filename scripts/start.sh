#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f infra/docker-compose.yml --profile core)
startup_timeout_seconds="${STARTUP_TIMEOUT_SECONDS:-120}"

wait_for() {
  local label="$1"
  shift
  local deadline=$((SECONDS + startup_timeout_seconds))
  until "$@"; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $label after ${startup_timeout_seconds}s." >&2
      return 1
    fi
    sleep 2
  done
}

taskmanager_ready() {
  curl -fsS http://localhost:8082/taskmanagers | python -c \
    'import json,sys; raise SystemExit(not json.load(sys.stdin).get("taskmanagers"))'
}

job_running() {
  curl -fsS http://localhost:8082/jobs/overview | python -c \
    'import json,sys; raise SystemExit(not any(job.get("state") == "RUNNING" for job in json.load(sys.stdin).get("jobs", [])))'
}

checkpoint_completed() {
  local job_id
  job_id="$(curl -fsS http://localhost:8082/jobs/overview | python -c \
    'import json,sys; jobs=json.load(sys.stdin).get("jobs", []); print(next((job["jid"] for job in jobs if job.get("state") == "RUNNING"), ""))')"
  [[ -n "$job_id" ]] || return 1
  curl -fsS "http://localhost:8082/jobs/$job_id/checkpoints" | python -c \
    'import json,sys; raise SystemExit(json.load(sys.stdin).get("counts", {}).get("completed", 0) < 1)'
}

clickhouse_ready() {
  "${compose[@]}" exec -T clickhouse sh -lc \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1"' \
    >/dev/null
}

if [[ "${RUNTIME_IMAGES_PREBUILT:-false}" == "true" ]]; then
  build_args=(--no-build)
else
  build_args=(--build)
fi

"${compose[@]}" up -d "${build_args[@]}" \
  kafka schema-registry clickhouse redis \
  flink-jobmanager flink-taskmanager
wait_for "ClickHouse" clickhouse_ready
"${compose[@]}" exec -T clickhouse sh -lc \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < infra/clickhouse/schema.sql
"${compose[@]}" run --rm kafka-init
"${compose[@]}" run --rm flink-checkpoint-init
"${compose[@]}" up -d redis-cart-materializer redis-feature-materializer
wait_for "Flink JobManager" curl -fsS http://localhost:8082/jobs/overview
wait_for "Flink TaskManager" taskmanager_ready
wait_for "Schema Registry" curl -fsS http://localhost:8081/subjects
bash scripts/register_schemas.sh
mkdir -p artifacts
"${compose[@]}" run --rm -e FLINK_PLAN_ONLY=true --entrypoint python \
  flink-submit /opt/flink/app/run.py > artifacts/flink-plan.txt
test -s artifacts/flink-plan.txt
echo "Flink planner validation passed: artifacts/flink-plan.txt"
"${compose[@]}" run --rm flink-submit
wait_for "running Flink job" job_running
wait_for "completed Flink checkpoint" checkpoint_completed
echo "Core is running with a completed checkpoint. Add --profile catalog or --profile api only for approved extensions."
