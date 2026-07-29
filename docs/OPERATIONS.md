# Operations and deployment boundary

## Status

The Docker Compose deployment is a **remote-host demo profile**. Its source,
schema, job submission, verification, recovery and teardown commands are
implemented. A full service run has not yet been captured as evidence, so it is
not deployment-verified or production-ready.

The final cloud target is Confluent Cloud for Kafka and Schema Registry. That
deployment is currently blocked: the committed ClickHouse Kafka Engine DDL uses
the local broker address `kafka:29092` and has no credential-injection path.
Do not attempt to point the existing local Compose profile at Confluent Cloud.

## Secrets

- Copy `.env.example` to `.env` only on the deployment host.
- Keep `.env`, cloud API keys, SASL passwords and Schema Registry credentials
  outside Git.
- Use the host's secret manager or protected environment variables for a
  managed Kafka deployment.
- Rotate a credential rather than editing it into JSON, SQL, Python or Compose.

## What a successful local-smoke run proves

- The fixture can travel through Kafka/Avro, the one SQL/PyFlink job and the
  ClickHouse/Redis materializers.
- Canonical raw, metrics, quality and active-cart results match the committed
  fixture contract.
- It does not prove cloud connectivity, sustained throughput, global
  exactly-once delivery or a production security posture.

## Operational checks

After `bash scripts/start.sh`, use:

```bash
docker compose -f infra/docker-compose.yml --profile core ps
curl -fsS http://localhost:8082/jobs/overview
curl -fsS http://localhost:8082/taskmanagers
```

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

## Failure handling and rollback

- A failed `scripts/start.sh` exits with the component that failed or timed
  out. Inspect `docker compose ... logs <service>` before retrying.
- For a failed Flink job, inspect the JobManager UI/logs and preserve the
  `flink_checkpoints` volume. Do not remove volumes before collecting evidence.
- The recovery experiment restarts only the TaskManager after a completed
  checkpoint and compares canonical snapshots.
- The legacy Java job is excluded rollback evidence, not a supported parallel
  deployment. Do not run it alongside the SQL/PyFlink job against the same
  topics or sinks.

## Teardown

`make stop` stops containers but retains named volumes and checkpoints for
inspection. The following command deletes all local runtime data and is only
appropriate after evidence is copied elsewhere:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile catalog --profile api down -v
```
