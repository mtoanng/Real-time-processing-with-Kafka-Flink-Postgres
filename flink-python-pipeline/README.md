# SQL/PyFlink job

This directory is the active Data Engineer authoring surface.

- `sql/`: source/sink contracts, validation, one-minute metrics and catalog
  current-state replication.
- `taobao_flink/operators.py`: bounded event-ID deduplication, late routing and
  active-cart keyed state.
- `taobao_flink/job.py`: attaches SQL sinks and DataStream operators to one
  `StreamExecutionEnvironment`.

`flink-sql-pipeline/` packages prebuilt JVM connectors only; it contains no
authored Java source. The former Java job under `flink-jobs/` is rollback-only
until a real parity and checkpoint-recovery run succeeds.
