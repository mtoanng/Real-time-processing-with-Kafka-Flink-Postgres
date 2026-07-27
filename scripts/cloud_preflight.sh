#!/usr/bin/env bash
set -euo pipefail

require() {
  if [ -z "${!1:-}" ]; then
    echo "ERROR: $1 is required" >&2
    exit 2
  fi
}

check_url() {
  local label="$1" url="$2" code
  code="$(curl --noproxy '*' --connect-timeout "${PREFLIGHT_TIMEOUT_SECONDS:-5}" --max-time "${PREFLIGHT_TIMEOUT_SECONDS:-5}" -sS -o /dev/null -w '%{http_code}' "$url" || true)"
  case "$code" in
    2*|3*|401|403) echo "$label: reachable" ;;
    *) echo "$label: FAILED (HTTP ${code:-no-response})"; return 1 ;;
  esac
}

check_tcp() {
  local label="$1" host="$2" port="$3"
  if command -v nc >/dev/null 2>&1; then
    nc -z -w "${PREFLIGHT_TIMEOUT_SECONDS:-5}" "$host" "$port" >/dev/null 2>&1
  else
    (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
  fi
  echo "$label: reachable"
}

require KAFKA_BOOTSTRAP_SERVERS
require SCHEMA_REGISTRY_URL
require CLICKHOUSE_ENDPOINT
runtime_profile="${RUNTIME_PROFILE:-core}"

KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS%:*}"
KAFKA_PORT="${KAFKA_BOOTSTRAP_SERVERS##*:}"
check_tcp "Kafka" "$KAFKA_HOST" "$KAFKA_PORT"
check_url "Schema Registry" "${SCHEMA_REGISTRY_URL%/}/subjects"
check_url "ClickHouse" "$CLICKHOUSE_ENDPOINT"
if [ "$runtime_profile" = serving ]; then
  require REDIS_HOST
  require REDIS_PORT
  require REDIS_CART_TTL_SECONDS
  check_tcp "Redis" "$REDIS_HOST" "$REDIS_PORT"
fi

if [ -n "${GRAFANA_ENDPOINT:-}" ]; then
  check_url "Grafana" "${GRAFANA_ENDPOINT%/}/api/health"
fi
if [ -n "${POSTGRES_HOST:-}" ]; then
  check_tcp "PostgreSQL" "$POSTGRES_HOST" "${POSTGRES_PORT:-5432}"
fi
if [ -n "${CONNECT_URL:-}" ]; then
  check_url "Kafka Connect" "${CONNECT_URL%/}/connectors"
fi

echo "Cloud preflight complete; no credential values were printed."
