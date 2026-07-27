# Latest Evidence Boundary

This directory is the active evidence location for the Redis serving
simplification migration.

- `expected-reconciliation.json` is generated independently from the committed
  fixture for replay attempts `fixture-run-a` and `fixture-run-b`.
- `PHASE_REPORT.md` records credential-independent commands and outcomes.
- `recovery/` is reserved for real uninterrupted/recovered snapshots and logs.

No live Kafka, Flink, ClickHouse, Redis, CDC, or cloud execution is claimed
unless corresponding artifacts are present here.
