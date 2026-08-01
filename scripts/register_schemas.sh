#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

registry="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
subject="${KAFKA_TOPIC:-user-behavior-events}-value"
auth=()
if [ -n "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:-}" ]; then
  auth=(-u "$SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
fi
schema="$(python -c 'import json; print(json.dumps(open("schemas/user-behavior-event.avsc", encoding="utf-8").read()))')"
payload="{\"schemaType\":\"AVRO\",\"schema\":$schema}"

curl -fsS "${auth[@]}" -X PUT "$registry/config/$subject" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility":"BACKWARD"}' >/dev/null
curl -fsS "${auth[@]}" -X POST "$registry/subjects/$subject/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "$payload" >/dev/null
echo "Registered $subject with BACKWARD compatibility."
