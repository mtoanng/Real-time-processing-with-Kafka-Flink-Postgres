# Current Implementation Status

> Archived pre-core-refactor status. See `docs/evidence/latest/`.

Audit boundary: credential-independent codebase completion. No cloud resources
were created and no live service result is claimed.

## Implemented and statically verified

- Deterministic raw-row replay, Avro/Kafka contracts, and fixture expectations.
- One Java Flink job with validation, timestamps/watermarks, late routing,
  one-minute item metrics, active-cart state, Broadcast State, and timers.
- Cassandra `local` and `astra` configuration using one shared Java driver sink,
  prepared idempotent upsert/delete statements, timeouts, lifecycle, and fixed
  lookup by `user_id`.
- ClickHouse `ReplacingMergeTree` logical keys, deterministic versions,
  deduplicated views, and durable invalid/late/alert tables.
- `checks`, `core`, and `full` profile contracts; core/full require Cassandra
  and default checkpointing on.
- Bounded independent reconciliation, CI definitions, Compose definitions,
  Terraform definitions, bootstrap, recovery, evidence, and teardown scripts.

## Implemented and locally verified

- Credential-independent Python and Java unit/contract tests and packaging.
- Ruff lint/format checks, Compose rendering, schema parsing, and Terraform
  formatting/validation with providers initialized.

This label does not mean that Kafka, Flink, ClickHouse, or Cassandra ran
locally; each command result is recorded in the final hardening report.

## Requires live deployment verification

- Kafka and Schema Registry permissions and delivery.
- Flink job submission, checkpoint creation, and recovery.
- ClickHouse connector writes, physical duplicates, and deduplicated query results.
- Local Cassandra or Astra sessions, mutations, lookup, retry, and restart behavior.
- PostgreSQL/Debezium live rule update, timer alert, and Grafana rendering.
- Cloud teardown, cost, latency, throughput, and scale.
- Bash syntax/execution on this Windows host; CI or a Linux deployment host must
  verify the shell entry points.

## Not implemented or not claimed

- Cross-system transactional exactly once.
- Production readiness or benchmarking.
- Multiple Flink jobs, Kubernetes, ML, recommendations, a frontend, or another
  serving database.
