# Execution Traces

This document follows concrete records through the code. Use it after the three
architecture layers to connect abstract guarantees to operator behavior.

## 1. Normal on-time event

Assume source sequence 0 contains:

```text
100,500,50,cart,1511658000
```

### Python

1. `iter_source_rows()` yields sequence `0` and five strings.
2. `parse_event()` converts IDs/timestamp and validates `cart`.
3. `deterministic_event_id()` hashes the five stable fields plus sequence 0.
4. `event_time_ms` becomes `1511658000000`.
5. `replay_run_id` is attached as lineage but is not hashed.
6. `KafkaEventPublisher.publish()` sends the Avro value with key `"100"`.

### Kafka and Schema Registry

1. The key selects a Kafka partition.
2. The Avro serializer writes a registry schema ID and encoded value.
3. Flink’s registry deserializer resolves the schema ID and creates a generated
   `UserBehaviorEvent`.

### Flink core

1. `EventValidator` passes positive IDs and non-negative times.
2. `keyBy(event_id)` routes all occurrences of this identity to one keyed
   deduplication state.
3. `EventDeduplicator` finds no unexpired state, writes first-seen processing
   time, and accepts the event.
4. The raw ClickHouse sink receives the accepted event.
5. Timestamp assignment uses `1511658000000`.
6. The watermark generator updates its maximum and emits
   `1511657994999`.
7. Since event time is greater than the watermark, `LateEventRouter` passes it.
8. `keyBy(ItemCategoryKey(500, 50))` selects the metric key.
9. The event enters the minute starting at `1511658000000`.
10. The accumulator increments `cart_count` and adds user 100.
11. When the watermark passes the window end, the window function emits one
    `ItemMetrics1m`.

### ClickHouse

The raw physical row is logically identified by date and event ID. The metric
row is logically identified by window start, item 500, and source category 50.
Canonical views expose one logical version of each.

## 2. Producer-rejected row

Assume a row has six columns or a non-integer timestamp.

1. `parse_event()` raises `RowValidationError`.
2. `iter_event_batches()` creates a `ParseIssue`.
3. `replay_file()` invokes `on_invalid`.
4. The CLI may write the issue to rejected JSONL.
5. No Avro record is produced.
6. Flink never sees the row.

Accounting consequence:

```text
attempted source rows = producer rejected rows + published rows
```

This rejection is not an `INVALID` Flink quality event.

## 3. Semantically invalid decoded event

The committed fixture deliberately allows an Avro-encodable event with a
non-positive ID to reach Flink.

1. Python parses the integer and creates an event ID.
2. Kafka transports and Flink decodes the event.
3. `EventValidator.invalidReason()` returns `IDs must be positive`.
4. The event does not enter the valid stream.
5. `StreamQualityEvent.fromEvent()` builds a deterministic
   `INVALID_IDENTIFIER` classification.
6. The quality sink writes it to `stream_quality_events`.
7. It never reaches deduplication, canonical raw, metrics, or optional serving.

## 4. Duplicate in the same or another replay attempt

Assume the same source row and sequence are published again.

1. Python calculates the same event ID even if `replay_run_id` changes.
2. Validation passes.
3. The same `event_id` key reaches the same logical dedup state.
4. If the state is within TTL, `EventDeduplicator` emits
   `DUPLICATE_WITHIN_RETENTION`.
5. The duplicate does not reach raw history, watermark advancement, metrics, or
   Cassandra.
6. Its quality ID includes the replay run ID, so separate attempts remain
   separately accountable.

If TTL has expired, the event can be accepted again. ClickHouse canonical raw
still converges on the stable event key, but duplicate quality is not emitted
for that post-expiry acceptance.

## 5. Out-of-order but still on-time event

Suppose the greatest timestamp observed is `10,000 ms`, so the emitted
watermark is `4,999 ms`. An event at `6,000 ms` arrives.

It is older than the maximum timestamp but newer than the watermark:

```text
6,000 > 4,999
```

The event is out of order but not late. It enters its event-time minute window.
This is the purpose of the five-second disorder allowance.

## 6. Late-for-aggregation event

Using the same watermark, an accepted unique event at `4,999 ms` arrives.

1. It has already passed validation and deduplication.
2. The raw sink receives it because it is real accepted history.
3. `LateEventRouter` sees `event_time <= watermark`.
4. It routes the event to `late-events`.
5. The mapping creates `LATE_FOR_AGGREGATION` quality.
6. The metric window never receives it.

This preserves:

```text
canonical_raw = on_time + late
```

while preventing a closed metric result from changing.

## 7. Metric aggregation with multiple users

Assume on-time events in one minute share item 500 and source category 50:

```text
user 100 cart
user 100 buy
user 101 pv
user 101 pv
```

The accumulator becomes:

```text
pv_count=2
cart_count=1
fav_count=0
buy_count=1
unique_users=2
```

The exact distinct set is `{100, 101}`. Four input events do not imply four
unique users.

An event for item 500 but category 51 goes to another key and another metric
row, even within the same minute.

## 8. Replaying the fixture twice

The expected static reconciliation uses two attempts:

```text
fixture-run-a
fixture-run-b
```

Both attempts contain 1,000 rows. Python rejects none. Each attempt includes
one semantic-invalid decoded event.

Across the two attempts:

```text
attempted source rows       2000
Flink decoded               2000
invalid                        2
valid                       1998
duplicate                    999
accepted unique              999
late                           2
on time                       997
canonical raw                999
canonical metric rows        995
```

Why duplicates equal 999 rather than 1,000:

- one invalid observation in each attempt is rejected before deduplication;
- the 999 valid identities from the first attempt are accepted;
- the same 999 valid identities in the second attempt are duplicates.

The two late values shown above reflect the independently predicted evidence
for this repository’s fixture and processing order. These are static expected
results until confirmed against a live pipeline.

## 9. Optional cart: cart then buy

For user 100 and item 500:

```text
cart at time 100, sequence 0
buy  at time 120, sequence 2
```

1. The cart creates active `CartItemState`, preserves added time 100, and emits
   an upsert.
2. Cassandra stores partition 100, clustering item 500.
3. The buy is newer, stores inactive state, and emits a delete.
4. Cassandra removes item 500 from user 100’s active cart.

The state remains inactive so a stale cart can be rejected later.

## 10. Optional cart: stale cart after buy

After the buy at `(120, 2)`, a cart for the same user/item arrives at `(110, 3)`.

`ActiveCartProjector.isStale()` compares event time first. Since 110 is less
than 120, the event produces neither next state nor mutation. The old cart
cannot recreate an item after the newer buy.

At equal event time, source sequence breaks the tie. An equal or lower sequence
is stale.

## 11. Optional cart: repeated mutation

Repeated external writes may occur around failure recovery.

- An upsert to the same `(user_id, item_id)` replaces the row deterministically.
- A repeated delete of the same primary key remains harmless.

This makes the Cassandra projection logically idempotent, but Cassandra is not
part of a distributed transaction with Flink.

## 12. Checkpoint and TaskManager failure

Assume a completed checkpoint contains Kafka offsets, dedup state, and open
windows.

1. A controlled command restarts a TaskManager without cancelling the job.
2. Flink restarts according to the fixed-delay policy.
3. Source offsets and operator state restore from a completed checkpoint.
4. Events processed after that checkpoint may be replayed.
5. Managed state and offsets remain consistent with the restored checkpoint.
6. External writes made after the checkpoint may be repeated.
7. Stable ClickHouse logical keys and canonical views collapse repeated
   physical writes.
8. The recovery experiment compares stable raw and metric snapshots with an
   uninterrupted fresh-environment baseline.

A service returning to `RUNNING` is not sufficient evidence. The final
business snapshots must match.

## 13. Physical versus canonical ClickHouse reads

Suppose a raw sink retry writes the same logical event twice.

Physical query:

```sql
SELECT count() FROM raw_behavior_events;
```

may temporarily return two rows.

Canonical query:

```sql
SELECT count() FROM raw_behavior_events_canonical;
```

uses `FINAL` and returns one logical row.

This is why operational diagnostics may inspect physical tables, while
business verification must use canonical views.

## 14. Deprecated rule/timer trace

This trace explains existing code only.

1. PostgreSQL changes `behavior_rules`.
2. Debezium unwraps and routes it to `behavior-rules`.
3. Flink accepts only a newer rule version into Broadcast State.
4. An on-time `cart` event stores a pending cart and registers an event-time
   timer.
5. A matching `buy` removes pending state.
6. If the timer fires while the rule remains enabled, the processor emits a
   `BehaviorAlert`.

This is not core quality processing and must not be treated as future product
CDC.

## 15. How to trace any new record yourself

For any fixture row, write down:

1. source sequence and stable event ID inputs;
2. producer acceptance or rejection;
3. Kafka key;
4. Flink semantic validity;
5. deduplication state decision;
6. raw-history decision;
7. watermark at arrival and late decision;
8. metric key and window;
9. optional user/item cart transition;
10. ClickHouse logical keys and expected quality records.

If every step is explicit, the event’s final outputs should be predictable
without running the system.

