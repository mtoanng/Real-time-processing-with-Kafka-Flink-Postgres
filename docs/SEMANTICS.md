# Stream semantics

- `event_id` is deterministic from source fields plus source sequence.
  `replay_run_id` is lineage only.
- `decoded = invalid + valid`.
- `valid = duplicate + accepted_unique` within the 168-hour default state TTL.
- `accepted_unique = on_time + late_for_aggregation`.
- Canonical raw contains all accepted unique events, including late events.
- Metrics contain on-time events only and use
  `(window_start, item_id, source_category_id)`.
- INVALID, DUPLICATE and LATE are durable deterministic quality records.
- Cart/fav/pv rules: `cart` upserts; `buy` deletes; `pv`/`fav` do nothing.
  `(event_time_ms, source_sequence)` prevents stale cart recreation.
- Catalog updates are keyed by `product_id` and monotonic
  `catalog_version`. Physical deletes are rejected; deactivate with
  `is_active=false`.

Flink checkpoints state and Kafka offsets consistently. Kafka output and
external materialization are at least once. ClickHouse replacement keys and
idempotent Redis hash mutations make canonical results converge; they do not
form a cross-system transaction.
