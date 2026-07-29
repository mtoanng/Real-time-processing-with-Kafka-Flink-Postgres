#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <git-sha>" >&2
  exit 2
fi

git_sha="$1"
repo_url="${REPO_URL:-https://github.com/mtoanng/Kafka-Flink-ClickHouse-Pipeline.git}"
deploy_root="${DEPLOY_ROOT:-$HOME/taobao-streaming}"
shared_dir="$deploy_root/shared"
release_dir="$deploy_root/releases/$git_sha"

if [[ ! "$git_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "git-sha must be a full 40-character lowercase commit SHA." >&2
  exit 2
fi
if [[ ! -f "$shared_dir/.env" ]]; then
  echo "Missing $shared_dir/.env; provision it from .env.example before deployment." >&2
  exit 1
fi

mkdir -p "$deploy_root/releases"
if [[ ! -d "$release_dir/.git" ]]; then
  git clone --quiet --no-checkout "$repo_url" "$release_dir"
fi

git -C "$release_dir" fetch --quiet --depth 1 origin "$git_sha"
git -C "$release_dir" checkout --quiet --detach "$git_sha"
ln -sfn "$shared_dir/.env" "$release_dir/.env"

export FLINK_RUNTIME_IMAGE="taobao-flink-runtime:$git_sha"
export APP_RUNTIME_IMAGE="taobao-app-runtime:$git_sha"
export RUNTIME_IMAGES_PREBUILT=true

(
  cd "$release_dir"
  docker compose -f infra/docker-compose.yml --profile core build --pull \
    flink-jobmanager redis-cart-materializer
  STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-240}" bash scripts/start.sh
)

ln -sfn "$release_dir" "$deploy_root/current"
printf '%s\n' \
  "git_sha=$git_sha" \
  "flink_image=$FLINK_RUNTIME_IMAGE" \
  "app_image=$APP_RUNTIME_IMAGE" \
  >"$shared_dir/current-release"

echo "Deployment completed: $git_sha"
