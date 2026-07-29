# End-to-end deployment runbook

## 0. Choose the correct host

Run this on a disposable Linux host or a remote Docker host, not the
resource-constrained laptop. The host needs Docker Engine with Compose v2,
Git, Bash, Python 3.11+, Maven, Java 11 and `curl`. The fixture is bounded; do
not copy the full Taobao dataset to the repository.

This runbook deploys the committed **local Docker Compose profile**. It does
not deploy Confluent Cloud or ClickHouse Cloud. See [operations](OPERATIONS.md)
for that explicit boundary.

## 1. Obtain and inspect the release candidate

```bash
git clone https://github.com/mtoanng/Kafka-Flink-ClickHouse-Pipeline.git
cd Kafka-Flink-ClickHouse-Pipeline
git checkout refactor/python-sql-preserve-architecture
git status --short
cp .env.example .env
```

`git status --short` must be empty before deployment. Edit only `.env`; do not
commit it. Keep the local defaults for this Compose profile. `KAFKA_SOURCE_BOUNDED`
must remain `false` for the normal start-then-replay sequence.

## 2. Run credential-independent gates

```bash
make checks
```

Expected: Python contracts, lint/format, compilation, connector packaging and
Compose rendering all succeed. These checks deliberately do not start Docker
services.

## 3. Build and start the core

```bash
STARTUP_TIMEOUT_SECONDS=180 bash scripts/start.sh
```

The script packages the connector bundle, starts Kafka, Schema Registry,
ClickHouse, Redis, the Redis materializer, Flink JobManager/TaskManager,
creates topics, registers Avro and submits one detached SQL/PyFlink job.

Verify the control plane before sending data:

```bash
docker compose -f infra/docker-compose.yml --profile core ps
curl -fsS http://localhost:8082/jobs/overview
curl -fsS http://localhost:8082/taskmanagers
```

Expected: one `RUNNING` job and at least one TaskManager. If not, stop here and
inspect logs; do not replay input into a failed job.

## 4. Run the bounded core E2E fixture

```bash
REPLAY_RUN_IDS=golden-a,golden-b bash scripts/replay.sh
PYTHONPATH=producer/src python scripts/verify.py
```

The verifier independently compares canonical raw events, one-minute metrics,
quality evidence and Redis active-cart state with committed fixture outputs.
It fails with a labelled expected/actual diff.

If the run is successful, capture an uninterrupted snapshot for recovery:

```bash
PYTHONPATH=producer/src python scripts/verify.py \
  --snapshot artifacts/uninterrupted.json --snapshot-only
```

The current late-event fixture expectation is a live verification item because
the active pipeline uses periodic built-in watermarks. Record the actual
result; do not alter golden data merely to make a service run pass.

## 5. Verify a completed checkpoint

Wait at least `FLINK_CHECKPOINT_INTERVAL_MS` after replay, then:

```bash
job_id="$(curl -fsS http://localhost:8082/jobs/overview | \
  python -c 'import json,sys; print(json.load(sys.stdin)["jobs"][0]["jid"])')"
curl -fsS "http://localhost:8082/jobs/$job_id/checkpoints"
```

The response must show a completed checkpoint before attempting recovery.

## 6. Optional catalog CDC extension

Start it only after the core verifier passes:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile catalog \
  up -d postgres kafka-connect
POSTGRES_PASSWORD=local-catalog python scripts/register_connector.py
bash scripts/update_catalog.sh
PYTHONPATH=producer/src python scripts/verify.py --with-catalog
```

This branch replicates current catalog state. It does not enrich behavior
history or change the metric source-category grain.

## 7. Optional HTTP/API extension

```bash
docker compose -f infra/docker-compose.yml --profile core --profile api up -d api
curl -fsS http://localhost:8000/health
curl -fsS 'http://localhost:8000/v1/users/1/cart'
curl -fsS 'http://localhost:8000/v1/products/trending?minutes=15&as_of_ms=1511658120000'
```

To publish through HTTP, omit `event_id`; the API recomputes it from stable
source fields:

```bash
curl -fsS -X POST http://localhost:8000/v1/events \
  -H 'content-type: application/json' \
  -d '{"user_id":9,"item_id":900,"category_id":90,"behavior_type":"cart","event_time_ms":1511658120000,"source_sequence":900,"replay_run_id":"http-demo"}'
```

## 8. Recovery experiment

Run this only on the disposable host after saving the baseline snapshot and
confirming a completed checkpoint:

```bash
RECOVERY_TEST_CONFIRM=YES \
FLINK_JOB_ID="$job_id" \
BASELINE_SNAPSHOT=artifacts/uninterrupted.json \
bash scripts/recovery_test.sh
```

Expected: the recovered canonical snapshot equals the uninterrupted snapshot.
This is not yet verified by repository evidence.

## 9. Collect evidence and teardown

Save command output, Flink checkpoint JSON and canonical snapshots under
`docs/evidence/final-e2e/` before teardown. That directory is intentionally
empty until a real run occurs.

```bash
mkdir -p docs/evidence/final-e2e
cp artifacts/uninterrupted.json docs/evidence/final-e2e/
curl -fsS "http://localhost:8082/jobs/$job_id/checkpoints" \
  > docs/evidence/final-e2e/checkpoints.json
make stop
```

`make stop` retains volumes. Follow [operations](OPERATIONS.md) only if you
intend to delete local runtime data.
