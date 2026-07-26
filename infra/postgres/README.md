# PostgreSQL control plane

> Deprecated legacy extension. This schema is retained for compatibility and
> is not the target CDC architecture or part of the core release.

`schema.sql` is the old control-plane schema. It contains one table,
`behavior_rules`; PostgreSQL is not a serving store for events or user state.
An approved future phase will replace this branch with Product Catalog CDC
Enrichment. That phase is not implemented or verified here.

Apply it only on a disposable remote demo PostgreSQL instance:

```bash
POSTGRES_HOST=... POSTGRES_DB=taobao_behavior \
  POSTGRES_USER=... PGPASSWORD=... \
  bash scripts/apply_behavior_rules_schema.sh
```

Operators publish a rule by inserting or updating a row. `version` must increase
for every logical update. Debezium snapshots existing rows and then streams future
updates to the compacted `behavior-rules` topic. Credentials are supplied through
the environment and are never committed.
