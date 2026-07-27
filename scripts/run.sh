#!/usr/bin/env bash
# Start the selected local runtime profile without printing credentials.
set -euo pipefail

cd "$(dirname "$0")/.."
compose_file="${COMPOSE_FILE:-infra/docker-compose.yml}"
runtime_profile="${RUNTIME_PROFILE:-core}"
runtime_dependencies="${RUNTIME_DEPENDENCIES:-local}"

case "$runtime_profile" in
  core|serving|cdc|observability) ;;
  checks)
    echo "RUNTIME_PROFILE=checks starts no services; run make checks" >&2
    exit 2
    ;;
  *)
    echo "ERROR: RUNTIME_PROFILE must be checks, core, serving, cdc, or observability" >&2
    exit 2
    ;;
esac

if [ "$runtime_profile" = "serving" ]; then
  : "${REDIS_HOST:?REDIS_HOST is required for serving}"
  : "${REDIS_PORT:?REDIS_PORT is required for serving}"
  : "${REDIS_CART_TTL_SECONDS:?REDIS_CART_TTL_SECONDS is required for serving}"
fi

if [ "$runtime_profile" = "cdc" ]; then
  echo "WARNING: cdc is a deprecated legacy compatibility profile, not a release target." >&2
  : "${POSTGRES_DB:?POSTGRES_DB is required for cdc}"
  : "${POSTGRES_USER:?POSTGRES_USER is required for cdc}"
  : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required for cdc}"
  : "${RULES_KAFKA_TOPIC:?RULES_KAFKA_TOPIC is required for cdc}"
fi

case "$runtime_dependencies" in
  local)
    services=(kafka schema-registry clickhouse flink-jobmanager flink-taskmanager)
    [ "$runtime_profile" = "serving" ] && services+=(redis)
    [ "$runtime_profile" = "cdc" ] && services+=(postgres debezium-connect)
    [ "$runtime_profile" = "observability" ] && services+=(grafana)
    docker compose -f "$compose_file" --profile "$runtime_profile" up -d "${services[@]}"
    echo "Local runtime dependencies started: $runtime_profile"
    ;;
  managed)
    echo "Managed runtime selected; no local containers were started."
    ;;
  *)
    echo "ERROR: RUNTIME_DEPENDENCIES must be local or managed" >&2
    exit 2
    ;;
esac
