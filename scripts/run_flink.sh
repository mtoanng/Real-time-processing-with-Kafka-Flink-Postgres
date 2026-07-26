#!/usr/bin/env bash
# Submit the single Taobao job to an existing remote Flink installation.
set -euo pipefail

cd "$(dirname "$0")/.."

FLINK_BIN="${FLINK_BIN:-flink}"
JAR_PATH="flink-jobs/taobao-stream-job/target/taobao-stream-job-1.0.0-SNAPSHOT.jar"

if ! command -v "$FLINK_BIN" >/dev/null 2>&1; then
  echo "ERROR: Flink CLI not found: ${FLINK_BIN}"
  exit 1
fi

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: JAR not found at ${JAR_PATH}"
  echo "Build it with: mvn -B -pl flink-jobs/taobao-stream-job -am package"
  exit 1
fi

echo "Submitting Taobao Flink job"
echo "  Runtime profile: ${RUNTIME_PROFILE:-core}"
echo "  Kafka: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
echo "  Topic: ${KAFKA_TOPIC:-user-behavior-events}"
echo "  Schema Registry: ${SCHEMA_REGISTRY_URL:-http://localhost:8081/apis/ccompat/v7}"
echo "  ClickHouse: ${CLICKHOUSE_ENDPOINT:-https://localhost:8443}"
echo "  Cassandra mode: ${CASSANDRA_MODE:-disabled}"
if [ "${RUNTIME_PROFILE:-core}" = "cdc" ]; then
  echo "  Deprecated legacy rules topic: ${RULES_KAFKA_TOPIC:-behavior-rules}"
fi

FLINK_ARGS=()
if [ "${FLINK_DETACHED:-false}" = "true" ]; then
  FLINK_ARGS+=("-d")
fi
if [ -n "${FLINK_JOBMANAGER:-}" ]; then
  FLINK_ARGS+=("-m" "$FLINK_JOBMANAGER")
fi

"$FLINK_BIN" run "${FLINK_ARGS[@]}" -c com.taobao.behavior.TaobaoStreamJob "$JAR_PATH"
