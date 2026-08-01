# Stream semantics

- `event_id` is deterministic from source fields plus source sequence.
  `replay_run_id` is lineage only.
- `decoded = invalid + valid`.
- `valid = duplicate + accepted_unique` within the 168-hour default SQL state
  TTL. Duplicate count is reconciled; individual duplicate audit rows are not
  emitted. Dedup keeps the first occurrence observed by Flink using processing
  time; event time is not used to choose a duplicate winner.
- `accepted_unique = on_time + late_for_aggregation`.
- Canonical raw contains all accepted unique events, including late events.
- Metrics contain on-time events only and use
  `(window_start, item_id, source_category_id)`.
- User features contain the same on-time input and use
  `(window_start, user_id)`. Counts cover one closed event-time minute;
  `feature_version` equals `window_end` in epoch milliseconds.
- An unbounded stream emits a window only after its watermark closes that
  window. Stopping a bounded replay does not synthesize a final watermark, so
  the last still-open minute is excluded from immediate verification.
- INVALID and LATE are durable deterministic quality records.
- Cart/fav/pv rules: latest `cart` upserts; latest `buy` deletes; `pv`/`fav` do
  nothing. SQL Top-1 ordered by `(event_time, source_sequence)` prevents stale
  cart recreation. `added_at_ms` is the latest cart-action time.
- ClickHouse retains every user feature snapshot for offline training. Redis
  key `taobao:features:user:{user_id}` retains only the greatest version and
  expires after `REDIS_FEATURE_TTL_SECONDS`; duplicate and stale writes are
  atomic no-ops.
- Catalog updates are keyed by `product_id` and monotonic
  `catalog_version`. Physical deletes are rejected; deactivate with
  `is_active=false`.
- The synthetic full-source catalog resolves multiple observed categories per
  item by highest frequency, then lowest category ID. This metadata policy
  never changes the original `source_category_id` used by behavior metrics.

Flink checkpoints state and Kafka offsets consistently. Kafka output and
external materialization are at least once. ClickHouse replacement keys and
idempotent Redis hash mutations make canonical results converge; they do not
form a cross-system transaction.
