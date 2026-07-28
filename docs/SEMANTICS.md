# Core semantics

## Identity and accounting

`event_id` is derived from stable source fields and `source_sequence`.
`replay_run_id` is lineage only and never participates in deduplication,
aggregation, or a ClickHouse logical key.

```text
decoded = invalid + valid
valid = duplicate + accepted_unique
accepted_unique = on_time + late_for_aggregation
canonical_raw = accepted_unique
metrics_input = on_time
```

Producer-side malformed CSV rows are reported separately and never claimed as
Flink-decoded rows.

## SQL processing

- Semantic validation rejects blank IDs, non-positive source identifiers or
  timestamps, and unsupported behavior values.
- A running occurrence count over `event_id` and processing-time arrival keeps
  the first row and classifies every later row as a duplicate.
- `table.exec.state.ttl` bounds deduplication state; the default horizon is
  seven days.
- Event time comes from `event_time_ms`; the default watermark is five seconds
  behind the maximum observed event time.
- Metrics use a one-minute tumbling event-time window at grain
  `(item_id, source_category_id)`.
- Late unique events remain in canonical raw history but do not update a closed
  metric window.

The SQL implementation emits durable `INVALID`, `DUPLICATE`, and `LATE`
classification contracts. Exact runtime changelog acceptance and the late
fixture result require the live Flink SQL experiment and are not inferred from
static tests.

## ClickHouse

Raw uniqueness is `event_id`. Metric uniqueness is
`(window_start, item_id, source_category_id)`. Quality uniqueness is
`quality_event_id`. Physical writes are at least once; canonical views use
`FINAL` over `ReplacingMergeTree`.

The ClickHouse Kafka Engine tables and materialized views are a platform sink
adapter. They are not business logic and require no Java authoring.

## Recovery guarantee

Flink `EXACTLY_ONCE` means completed checkpoints consistently capture Flink
state and Kafka source positions. It does not make ClickHouse part of the same
transaction. Stable logical identities plus ClickHouse replacement semantics
address external retry duplicates.
