# Operations and deployment boundary

## Status

The Docker Compose deployment is the **AWS EC2 self-hosted demonstration
profile**. It runs Apache Kafka KRaft, Confluent Schema Registry, Flink,
ClickHouse and Redis on one disposable instance. Its source, schema, job
submission, verification, recovery and teardown commands are implemented. A
full service run has not yet been captured as evidence, so it is not
deployment-verified or production-ready.

`kafka:29092` is intentional: it is the internal Docker hostname used by
ClickHouse Kafka Engine and Flink on the EC2 instance. Kafka's host port is
bound to loopback, so it is not an Internet-facing broker.

## Release model

- `.github/workflows/ci.yml` is both the credential-independent merge gate and
  the small single-host deployment workflow.
- Pull requests run checks only. A successful push to `main`, or a manual
  dispatch, enters the protected GitHub Environment `aws-demo`.
- GitHub opens one non-interactive SSH session and streams
  `scripts/deploy_ec2.sh` to the host. The private key exists only as a GitHub
  Environment secret and should be dedicated to deployment.
- The host checks out the exact SHA under `~/taobao-streaming/releases/`,
  builds both runtime images with that SHA as the local tag, starts the stack,
  and advances `current` only after startup succeeds.
- `~/taobao-streaming/shared/.env`, Docker volumes and Flink checkpoints
  remain outside the release directory.

This is a single-host demonstration release model, not autoscaling,
high-availability orchestration.

## Secrets

- Copy `.env.example` to `.env` only on the deployment host.
- Keep `.env`, the SSH deploy key, passwords and API tokens outside Git.
- Store the deployment `.env` at `~/taobao-streaming/shared/.env` with mode
  `600`. Use Session Manager or restricted SSH to provision it.
- Use restrictive EC2 security-group rules; no application secret needs to be
  exposed publicly for the self-hosted demo.
- Rotate a credential rather than editing it into JSON, SQL, Python or Compose.

## What a successful local-smoke run proves

- The fixture can travel through Kafka/Avro, the one Table/SQL job and the
  ClickHouse/Redis materializers.
- Canonical raw, metrics, quality, offline feature history, online Redis
  features and active-cart results match the committed fixture contract.
- It does not prove cloud connectivity, sustained throughput, global
  exactly-once delivery or a production security posture.

## Operational checks

After `bash scripts/start.sh`, use:

```bash
docker compose -f infra/docker-compose.yml --profile core ps
curl -fsS http://localhost:8082/jobs/overview
curl -fsS http://localhost:8082/taskmanagers
```

`start.sh` first runs `StatementSet.explain()` and writes the optimized plan to
`artifacts/flink-plan.txt`. A source-to-sink changelog incompatibility therefore
fails startup before the real job is submitted or any replay is allowed.
It also waits for the first completed checkpoint before reporting success.

The job must be `RUNNING` and at least one TaskManager must be registered.
After replay, wait for the configured checkpoint interval, then inspect:

```bash
job_id="$(curl -fsS http://localhost:8082/jobs/overview | \
  python -c 'import json,sys; print(json.load(sys.stdin)["jobs"][0]["jid"])')"
curl -fsS "http://localhost:8082/jobs/$job_id/checkpoints"
```

Use canonical ClickHouse views only:

```bash
curl -u "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-local-clickhouse}" \
  'http://localhost:8123/?database=taobao_behavior' \
  --data-binary 'SELECT count() FROM raw_behavior_events_canonical'
```

Inspect feature materialization independently:

```bash
curl -u "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-local-clickhouse}" \
  'http://localhost:8123/?database=taobao_behavior' \
  --data-binary 'SELECT count() FROM user_features_1m_canonical'
docker compose -f infra/docker-compose.yml exec -T redis \
  redis-cli HGETALL 'taobao:features:user:{1}'
```

For a full-dataset catalog, retain
`artifacts/product_catalog_manifest.json` with the deployment evidence. Its
`unique_products` must equal both PostgreSQL `product_catalog` and ClickHouse
`product_catalog_current_canonical` counts. The generated names and prices are
synthetic demonstration metadata, not Alibaba source attributes.

Catalog replication has a separate failure boundary from the behavior job:
PostgreSQL -> Debezium -> Kafka -> ClickHouse Kafka Engine. Flink is not an
intermediate CDC transport.

## Failure handling and rollback

- A failed `scripts/start.sh` exits with the component that failed or timed
  out. Inspect `docker compose ... logs <service>` before retrying.
- For a failed Flink job, inspect the JobManager UI/logs and preserve the
  `flink_checkpoints` volume. Do not remove volumes before collecting evidence.
- Roll back by manually dispatching a known-good commit from its GitHub ref.
  The host rebuilds source and both local images from that exact SHA.
- The recovery experiment restarts only the TaskManager after a completed
  checkpoint and compares canonical snapshots.
- Do not run inactive jobs alongside the active job against the same topics or
  sinks.

## Teardown

`make stop` stops containers but retains named volumes and checkpoints for
inspection. The following command deletes all local runtime data and is only
appropriate after evidence is copied elsewhere:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile catalog --profile api down -v
```
