# Stream semantics

## Contract and identity

The source is the raw Alibaba Tianchi five-column row:

```text
user_id,item_id,category_id,behavior_type,timestamp
```

Python rejects malformed CSV or values that cannot be encoded by the Avro
contract. Flink classifies decoded semantic failures.

```text
event_id = SHA-256(
  user_id, item_id, category_id, behavior_type, timestamp, source_sequence
)
```

Fields are joined in that order with the unit-separator byte. `replay_run_id`
is lineage only. Kafka is keyed by `user_id`.

## Accounting and deduplication

For a bounded replay:

```text
decoded = invalid + valid
valid = duplicate + accepted_unique
accepted_unique = on_time + late_for_aggregation
canonical_raw = accepted_unique
metrics_input = on_time
```

Flink keys valid events by `event_id`. State TTL bounds uniqueness to
`FLINK_DEDUP_RETENTION_HOURS`; it is not eternal global uniqueness. A repeated
valid event inside that horizon creates one deterministic `DUPLICATE` quality
identity for its replay attempt and does not reach raw, metrics, or Redis again.

## Event time and metrics

The watermark is the maximum observed accepted event time minus
`FLINK_MAX_OUT_OF_ORDERNESS_MS` minus one millisecond. An event at or behind the
watermark is late:

- it remains in canonical raw history;
- it creates one `LATE_FOR_AGGREGATION` quality event;
- it does not update metrics.

On-time events enter one-minute tumbling windows keyed by
`(item_id, source_category_id)`. The materialized metric identity is
`(window_start, item_id, source_category_id)` and contains exact `pv`, `cart`,
`fav`, `buy`, and distinct-user counts.

## Materialized outputs

ClickHouse replacement tables can contain retry copies. The canonical views use
`FINAL` and stable logical keys:

- raw: event date and `event_id`;
- metrics: window start, item, and source category;
- quality: deterministic `quality_event_id`.

Redis stores one Hash per user. The field is `item_id`; the value is
`category_id|added_at_ms|last_updated_at_ms`. `cart` upserts, `buy` deletes,
and `pv`/`fav` do nothing. Event time and source sequence reject stale
transitions. Every existing active-cart key has a bounded TTL. A buy without a
prior cart is a harmless delete.

## Delivery boundary

Flink checkpoints coordinate Flink state and Kafka offsets. ClickHouse and
Redis are external sinks. Deterministic identifiers, canonical keys, and
idempotent cart mutations make logical outputs converge after retry. There is
no transaction spanning Kafka, Flink, ClickHouse, and Redis, so the complete
platform is not globally exactly once.

## Golden contract

Two attempts of the 12-row fixture produce:

```text
decoded=24, invalid=2, valid=22
duplicate=11, accepted_unique=11
on_time=10, late_for_aggregation=1
canonical_raw=11, metrics_input=10
```

The exact raw rows, five metric rows, fourteen quality identities, and final
cart values are manually listed in `tests/fixtures/golden_outputs.json`.
