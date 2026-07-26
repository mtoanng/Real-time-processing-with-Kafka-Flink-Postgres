# Flink API Strategy

> Archived pre-core-refactor audit; not an active implementation contract.

## What Could Use Flink SQL

Several parts of the repository could be expressed with Flink SQL in a team
that standardizes on SQL/Table API:

- Kafka source and Avro format declarations;
- timestamp extraction and a five-second watermark;
- filtering valid behavior values;
- one-minute tumbling windows grouped by `replay_run_id`, `item_id`, and
  `category_id` with counts and distinct users;
- inserts into ClickHouse through a supported connector;
- simple projections into a narrow event-history table.

An illustrative SQL shape for the analytical branch would be:

```sql
SELECT
  window_start,
  item_id,
  category_id,
  COUNT(*) FILTER (WHERE behavior_type = 'pv') AS pv_count,
  COUNT(*) FILTER (WHERE behavior_type = 'cart') AS cart_count,
  COUNT(*) FILTER (WHERE behavior_type = 'fav') AS fav_count,
  COUNT(*) FILTER (WHERE behavior_type = 'buy') AS buy_count,
  COUNT(DISTINCT user_id) AS unique_users
FROM TABLE(
  TUMBLE(TABLE behavior_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, item_id, category_id;
```

That is a conceptual example, not a repository pipeline. It does not replace
the Java job and should not be submitted as a second active implementation.

## Why This Repository Uses Java DataStream

The actual job is `TaobaoStreamJob.java` and uses APIs that are clearer and
more directly testable in Java for this project’s extension surface:

- generated specific Avro classes and typed `DataStream<UserBehaviorEvent>`;
- typed `OutputTag` side outputs for invalid and late events;
- a custom per-event watermark generator with deterministic emission;
- `KeyedProcessFunction` plus `MapState` for active-cart lifecycle and stale
  event rejection;
- `KeyedBroadcastProcessFunction` with `BroadcastState` for versioned rules;
- event-time timers for the optional cart-abandonment alert branch;
- explicit operator names and UIDs;
- a Java Cassandra driver sink with prepared statements and lifecycle methods.

These are not impossible in SQL, but they would require Table API extensions,
SQL connector capabilities, or a mixed SQL/DataStream job. The current Java
topology keeps the control flow, state transitions, side outputs, and test
fixtures visible to a junior engineer.

## Where SQL Should Be Preferred in a Real Team

Prefer Flink SQL when the transformation is relational and stable:

- filtering and projection;
- schema-driven joins;
- standard tumbling, hopping, or session windows;
- common aggregations;
- governed sinks with mature SQL connectors;
- work owned by analysts and platform engineers who need explainable SQL.

Prefer Java DataStream or a small custom operator when behavior depends on
custom state transitions, precise side-output contracts, nonstandard ordering,
driver lifecycle, or a library not exposed cleanly through SQL. In a real team,
the choice should be based on connector maturity, operational ownership,
testing, observability, and whether the SQL planner expresses the required
semantics without hidden custom code.

## Decision for Project 1

Keep the single Java DataStream job. Do not create a second active Flink SQL
pipeline. SQL remains a reasonable future expression for the ClickHouse
analytical branch, while the active-cart and broadcast-state branches are
currently documented against the Java implementation.
