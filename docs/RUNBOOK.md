# Runbook

## Prerequisites

- Python 3.11+ and the `kafka` optional dependencies
- Java 11
- Maven 3.9+
- Docker Compose v2
- Flink CLI 1.20.2 when submitting to the local Compose cluster
- Terraform 1.9+ for static infrastructure validation

Do not run the complete stack on the constrained laptop. Use a disposable host.

## Credential-independent checks

```bash
PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v
ruff check producer scripts
ruff format --check producer scripts
mvn -B -pl flink-jobs/taobao-stream-job -am package
docker compose -f infra/docker-compose.yml --profile core config --quiet
docker compose -f infra/docker-compose.yml --profile serving config --quiet
# Deprecated legacy compatibility render only; not a release target.
docker compose -f infra/docker-compose.yml --profile cdc config --quiet
docker compose -f infra/docker-compose.yml --profile observability config --quiet
terraform -chdir=infra/terraform init -backend=false -input=false
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform validate
```

## Fresh core demo

Use a fresh ClickHouse volume/database because schema bootstrap is deliberately
non-destructive and does not alter legacy tables.

```bash
cp .env.example .env
set -a; source .env; set +a
RUNTIME_PROFILE=core bash scripts/run.sh
PYTHONPATH=producer/src python scripts/apply_clickhouse_schema.py
bash scripts/register_schemas.sh
mvn -B -pl flink-jobs/taobao-stream-job -am package
FLINK_DETACHED=true bash scripts/run_flink.sh
bash scripts/run_replay_identity_experiment.sh
```

## Deprecated legacy CDC

The checked-in `cdc` profile exists only to keep the old `behavior_rules`
implementation isolated and compilable. Do not use it as the target
architecture for this release. Product Catalog CDC Enrichment is **NOT
IMPLEMENTED** and has no run command in this runbook.

## Serving

Set `RUNTIME_PROFILE=serving` and configure a local or managed Redis-compatible
endpoint with `REDIS_HOST`, `REDIS_PORT`, optional ACL credentials/TLS, key
prefix, and cart TTL. Then:

```bash
bash scripts/run.sh
FLINK_DETACHED=true bash scripts/run_flink.sh
bash scripts/run_replay_identity_experiment.sh
```

## Recovery

Follow the printed two-environment workflow:

```bash
bash scripts/run_checkpoint_experiment.sh
```

The recovery environment requires `curl`, `jq`, `FLINK_REST_URL`,
`FLINK_JOB_ID`, a persistent checkpoint path, and a failure command that
restarts a TaskManager without cancelling the job.

## Teardown

Inspect the Compose project, then stop it:

```bash
docker compose -f infra/docker-compose.yml ps
bash scripts/stop.sh
```

Only use `bash scripts/stop.sh --volumes` when local ClickHouse, checkpoint,
Redis, and PostgreSQL data may be discarded.
