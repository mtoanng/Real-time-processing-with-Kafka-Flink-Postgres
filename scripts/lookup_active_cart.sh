#!/usr/bin/env bash
# Read one user's active-cart Hash through the Redis-compatible client.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$#" -ne 1 ] || ! [[ "$1" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: bash scripts/lookup_active_cart.sh <positive-user-id>"
  exit 2
fi

JAR_PATH="flink-jobs/taobao-stream-job/target/taobao-stream-job-1.0.0-SNAPSHOT.jar"
if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: JAR not found at ${JAR_PATH}"
  echo "Build it with: mvn -B -pl flink-jobs/taobao-stream-job -am package"
  exit 1
fi

exec java -cp "$JAR_PATH" com.taobao.behavior.ActiveCartLookupCli --user-id "$1"
