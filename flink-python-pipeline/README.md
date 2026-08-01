# Flink job

This directory contains the active Flink job.

- `sql/`: behavior source/sink contracts, validation and one-minute metrics
  current-state replication.
- `taobao_flink/operators.py`: bounded event-ID deduplication, late routing and
  active-cart keyed state.
- `taobao_flink/job.py`: attaches SQL sinks and DataStream operators to one
  `StreamExecutionEnvironment`.

`flink-connectors/` packages the pinned connector dependencies used by the
runtime image.
