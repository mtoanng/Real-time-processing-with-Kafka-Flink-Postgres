#!/usr/bin/env bash
set -euo pipefail

# Amazon Linux 2023 bootstrap. URLs and checksums are inputs; credentials are
# supplied later through /etc/taobao-runtime.env and are never written here.
APP_ROOT=/opt/taobao
RUNTIME_ROOT="$APP_ROOT/runtime"
FLINK_ROOT="$APP_ROOT/flink-${flink_version}"
ENV_FILE=/etc/taobao-runtime.env

dnf install -y docker git python3 java-11-amazon-corretto-headless tar gzip curl jq
systemctl enable --now docker
usermod -aG docker ec2-user
install -d -m 0755 "$APP_ROOT" "$RUNTIME_ROOT" /etc/taobao /var/lib/taobao-flink/checkpoints

download_checked() {
  local url="$1" destination="$2" checksum="$3"
  curl --fail --location --retry 3 --proto '=https' --tlsv1.2 "$url" -o "$destination"
  if [ -n "$checksum" ]; then
    printf '%s  %s\n' "$checksum" "$destination" | sha256sum -c -
  fi
}

if [ ! -x "$FLINK_ROOT/bin/flink" ]; then
  archive="$APP_ROOT/flink.tgz"
  download_checked "${flink_download_url}" "$archive" "${flink_sha256}"
  tar -xzf "$archive" -C "$APP_ROOT"
  test -x "$FLINK_ROOT/bin/flink"
  ln -sfn "$FLINK_ROOT" "$APP_ROOT/flink"
  rm -f "$archive"
fi

if [ -n "${runtime_bundle_url}" ]; then
  bundle="$APP_ROOT/runtime-bundle.tgz"
  download_checked "${runtime_bundle_url}" "$bundle" ""
  tar -xzf "$bundle" -C "$RUNTIME_ROOT" --strip-components=1
  rm -f "$bundle"
fi

if [ -n "${application_jar_url}" ]; then
  install -d -m 0755 "$RUNTIME_ROOT/flink-jobs/taobao-stream-job/target"
  download_checked "${application_jar_url}" \
    "$RUNTIME_ROOT/flink-jobs/taobao-stream-job/target/taobao-stream-job-1.0.0-SNAPSHOT.jar" \
    "${application_jar_sha256}"
fi

cat > "$ENV_FILE" <<'RUNTIME_ENV'
# Operator-owned runtime configuration. Add endpoints and secrets here after bootstrap.
RUNTIME_PROFILE=${runtime_profile}
RUNTIME_DEPENDENCIES=managed
FLINK_BIN=/opt/taobao/flink/bin/flink
FLINK_CHECKPOINTING_ENABLED=true
FLINK_CHECKPOINT_INTERVAL_MS=60000
FLINK_CHECKPOINT_DIR=file:///var/lib/taobao-flink/checkpoints
FLINK_RESTART_ATTEMPTS=3
FLINK_RESTART_DELAY_MS=10000
FLINK_DEDUP_RETENTION_HOURS=168
COMPOSE_FILE=/opt/taobao/runtime/infra/docker-compose.yml
RUNTIME_ROOT=/opt/taobao/runtime
START_FLINK=false
RUNTIME_ENV
chmod 0600 "$ENV_FILE"

cat > /usr/local/bin/taobao-runtime-start <<'START'
#!/usr/bin/env bash
set -euo pipefail
source /etc/taobao-runtime.env
cd "$${RUNTIME_ROOT:?RUNTIME_ROOT is required}"
test -f "$COMPOSE_FILE"
case "$${RUNTIME_PROFILE:?RUNTIME_PROFILE is required}" in
  core|serving|cdc|observability) ;;
  *) echo "Unsupported RUNTIME_PROFILE" >&2; exit 2 ;;
esac
bash scripts/run.sh
if [ "$${START_FLINK:-false}" = true ]; then
  bash scripts/run_flink.sh
fi
bash scripts/runtime_healthcheck.sh
START
chmod 0755 /usr/local/bin/taobao-runtime-start

cat > /usr/local/bin/taobao-runtime-stop <<'STOP'
#!/usr/bin/env bash
set -euo pipefail
source /etc/taobao-runtime.env
cd "$${RUNTIME_ROOT:?RUNTIME_ROOT is required}"
bash scripts/stop.sh
STOP
chmod 0755 /usr/local/bin/taobao-runtime-stop

if [ "${auto_start_runtime}" = true ] && [ -f "$${RUNTIME_ROOT}/infra/docker-compose.yml" ] && \
   [ -f "$RUNTIME_ROOT/flink-jobs/taobao-stream-job/target/taobao-stream-job-1.0.0-SNAPSHOT.jar" ]; then
  /usr/local/bin/taobao-runtime-start || logger -t taobao-bootstrap "runtime start deferred; inspect host logs"
fi
