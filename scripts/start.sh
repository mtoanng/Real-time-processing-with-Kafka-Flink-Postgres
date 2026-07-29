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

if [[ "${RUNTIME_IMAGES_PREBUILT:-false}" == "true" ]]; then
  build_args=(--no-build)
else
  build_args=(--build)
fi

"${compose[@]}" up -d "${build_args[@]}" \
  kafka schema-registry clickhouse redis redis-cart-materializer \
  flink-jobmanager flink-taskmanager
wait_for "Flink JobManager" curl -fsS http://localhost:8082/jobs/overview
wait_for "Flink TaskManager" taskmanager_ready
wait_for "Schema Registry" curl -fsS http://localhost:8081/apis/ccompat/v7/subjects
"${compose[@]}" run --rm kafka-init
bash scripts/register_schemas.sh
"${compose[@]}" run --rm flink-submit
wait_for "running Flink job" job_running
echo "Core is running. Add --profile catalog or --profile api only for approved extensions."
