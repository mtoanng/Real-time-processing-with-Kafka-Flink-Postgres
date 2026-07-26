# Cloud Deployment Runbook

> Archived pre-core-refactor contract. See `docs/RUNBOOK.md`.

Status: **requires live deployment verification**. This runbook does not
authorize or perform cloud provisioning.

## Target

- one disposable remote Flink 1.20.2 host/cluster;
- Confluent-compatible Kafka and Schema Registry;
- ClickHouse endpoint;
- Apache Cassandra in `astra` mode;
- optional PostgreSQL/Debezium/Grafana for `full`.

## Prepare artifacts

```bash
make test
make package
make infra-config
terraform -chdir=infra/terraform init -backend=false -input=false
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
```

Publish the shaded JAR and a versioned runtime bundle through an operator-owned
artifact location. Record SHA-256 values. Do not place credentials in either
artifact or Terraform user data.

Use a fresh ClickHouse database for the first deployment of this schema. The
schema installer uses `CREATE ... IF NOT EXISTS`; it deliberately does not alter
or drop an older table. An existing deployment therefore needs a separately
reviewed migration or a new database followed by bounded replay.

## Review infrastructure without applying

```bash
terraform -chdir=infra/terraform plan -refresh=false -var-file=terraform.tfvars
```

A successful plan is not deployment evidence. `terraform apply` requires a
separate explicit operator decision outside repository checks.

## Private runtime configuration

Start from `.env.cloud.example`. Set `RUNTIME_PROFILE=core` or `full`,
`RUNTIME_DEPENDENCIES=managed`, and `CASSANDRA_MODE=astra`. Supply Kafka,
Schema Registry, ClickHouse, Secure
Connect Bundle, Astra token, checkpoint storage, and full-profile credentials
through a private environment file or secret manager.

The runtime identity needs topic read/write/describe, Flink consumer-group
read, and Schema Registry subject access. Debezium additionally needs compacted
rules and Kafka Connect internal-topic permissions.

## Deploy and verify

On the prepared host:

```bash
bash scripts/cloud_preflight.sh
bash scripts/apply_cassandra_schema.sh
PYTHONPATH=producer/src python scripts/apply_clickhouse_schema.py
bash scripts/register_schemas.sh
FLINK_DETACHED=true bash scripts/run_flink.sh
bash scripts/replay.sh
bash scripts/verify_bounded_pipeline.sh "$REPLAY_RUN_ID"
bash scripts/collect_cloud_evidence.sh
```

For `full`, register Debezium, make one higher-version rule update, and query
`behavior_alerts_deduplicated`. Capture counts and logs without credentials.

## Rollback

Stop the candidate job, retain its checkpoints, and resubmit the last known JAR
only from a compatible checkpoint. If state compatibility is uncertain, use a
new consumer group/run ID and bounded replay rather than guessing. ClickHouse
deduplicated views and Cassandra primary-key writes make repeated bounded input
observable and convergent, but do not form a cross-system transaction.

## Teardown

```bash
CONFIRM_TEARDOWN=YES bash scripts/teardown_demo.sh
bash scripts/verify_cloud_teardown.sh
```

Provider consoles must also be checked. Local commands cannot prove that all
managed resources and billing objects were deleted.
