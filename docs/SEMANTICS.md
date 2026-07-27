# Stream Semantics

## Identity

`event_id = SHA-256(user_id, item_id, category_id, behavior_type, timestamp,
source_sequence)`.

The replay run is intentionally absent. A retry or a second replay attempt of
the same source position produces the same ID.

`replay_run_id` answers “which attempt observed this record?” It never changes
business grouping or uniqueness.

## Accounting

```text
decoded = invalid + valid
valid = duplicate + accepted_unique
accepted_unique = on_time + late_for_aggregation
canonical_raw = accepted_unique
metrics_input = on_time
```

Python rejects malformed or non-Avro-encodable rows before publication and
reports them separately as `producer_rejected_rows`. Flink quality events cover
only decoded records.

## Deduplication

Valid events are keyed by `event_id`. `EventDeduplicator` stores first-seen
processing time in checkpointed `ValueState` with Flink State TTL.

Default:

```text
FLINK_DEDUP_RETENTION_HOURS=168
```

The guarantee is “one accepted occurrence per event ID within the configured
retention horizon,” not eternal global uniqueness. After expiry, the event can
be accepted again; ClickHouse replacement keys still protect canonical reads.

## Event time

Accepted unique events enter canonical raw before aggregation lateness is
classified. A five-second bounded-out-of-orderness watermark determines whether
an event can still update a one-minute window.

A late event:

- remains in canonical raw;
- emits one `LATE_FOR_AGGREGATION` quality classification;
- does not enter metrics.

Metric identity:

```text
(window_start, item_id, source_category_id)
```

`source_category_id` is copied from the original Taobao event. Future catalog
enrichment must not replace it or change core metric results.

## Quality identity

`quality_event_id` deterministically includes quality type, event ID, replay
attempt, source sequence, and reason code. Reprocessing the same classification
within one attempt replaces it; a later replay attempt remains separately
accountable.

Core quality types:

- `INVALID`
- `DUPLICATE`
- `LATE`

Deprecated legacy behavior alerts remain separate and are not core quality
evidence.

## Delivery guarantees

`CheckpointingMode.EXACTLY_ONCE` coordinates Flink-managed state and Kafka
offset recovery. It does not transactionally commit ClickHouse or Redis.

ClickHouse uses `ReplacingMergeTree` plus `FINAL` canonical views. Redis uses
deterministic Hash `HSET`/`HDEL` mutations after Flink rejects stale cart
transitions. The combined platform is recoverable and effectively once at
logical query boundaries, not globally transactional exactly once.
