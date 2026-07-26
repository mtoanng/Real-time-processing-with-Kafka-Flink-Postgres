#!/usr/bin/env bash
# Bounded local runtime health check; credentials are never printed.
set -euo pipefail

cd "$(dirname "$0")/.."
compose_file="${COMPOSE_FILE:-infra/docker-compose.yml}"
runtime_profile="${RUNTIME_PROFILE:-core}"
case "$runtime_profile" in
  core|serving|cdc|observability) ;;
  *) echo "ERROR: unsupported runtime profile: $runtime_profile" >&2; exit 2 ;;
esac
profiles=(--profile "$runtime_profile")
if [ "${RUNTIME_DEPENDENCIES:-local}" = local ]; then
  docker compose -f "$compose_file" "${profiles[@]}" ps --status running --services | sort
fi

schema_registry_url="${SCHEMA_REGISTRY_URL:-http://localhost:8081/apis/ccompat/v7}"
curl --fail --silent --show-error --max-time "${HEALTHCHECK_TIMEOUT_SECONDS:-5}" \
  "${schema_registry_url%/}/subjects" >/dev/null

if [ -n "${CLICKHOUSE_ENDPOINT:-}" ]; then
  curl --fail --silent --show-error --max-time "${HEALTHCHECK_TIMEOUT_SECONDS:-5}" \
    "${CLICKHOUSE_ENDPOINT%/}/?query=SELECT%201" >/dev/null
fi

if [ -n "${FLINK_REST_URL:-}" ]; then
  curl --fail --silent --show-error --max-time "${HEALTHCHECK_TIMEOUT_SECONDS:-5}" \
    "${FLINK_REST_URL%/}/overview" >/dev/null
fi

echo "Runtime health checks passed."
