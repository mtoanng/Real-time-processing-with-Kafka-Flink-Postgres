# Codebase Index

```text
raw Taobao rows -> Python -> Kafka/Avro -> one Java Flink job -> ClickHouse
```

Optional release profiles add Valkey/Redis serving or Grafana. Deprecated
PostgreSQL/Debezium behavior-rule artifacts remain isolated for a later
replacement migration.

| Area | Location |
| --- | --- |
| Replay package and tests | `producer/` |
| Java Flink job and tests | `flink-jobs/taobao-stream-job/` |
| Avro contracts | `schemas/` |
| ClickHouse DDL and queries | `infra/clickhouse/` |
| Optional Valkey/Redis | `infra/docker-compose.yml`, Java `sink/Redis*` classes |
| Deprecated legacy CDC | `infra/postgres/`, `infra/debezium/`, `infra/kafka/` |
| Compose and Terraform | `infra/docker-compose.yml`, `infra/terraform/` |
| Verification and experiments | `scripts/` |
| Deterministic fixture | `tests/fixtures/` |

Active documentation:

- `README.md`
- `docs/PROJECT1_BLUEPRINT_FINAL.md`
- `docs/ARCHITECTURE.md`
- `docs/SEMANTICS.md`
- `docs/RUNBOOK.md`
- `docs/OPERATIONS.md`
- `docs/learning/`
- `docs/evidence/latest/`

Everything under `docs/archive/` is historical and not an active contract.
