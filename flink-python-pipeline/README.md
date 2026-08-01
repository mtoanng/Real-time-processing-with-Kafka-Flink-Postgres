# Flink job

This directory contains the active Flink job.

- `sql/`: source/sink contracts, validation, bounded deduplication, native
  watermarks, late routing, one-minute metrics, one-minute user features and
  latest cart state.
- `taobao_flink/job.py`: creates a Python `TableEnvironment`, configures
  checkpoint/restart policy and submits all SQL inserts as one `StatementSet`.

No Python UDF or DataStream callback runs per event. Python is the submission
and integration language; Flink executes the relational plan in the JVM.

`flink-connectors/` packages the pinned connector dependencies used by the
runtime image.
