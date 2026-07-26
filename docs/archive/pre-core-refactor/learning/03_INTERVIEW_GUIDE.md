# Interview and Presentation Guide

> Archived learning material for the pre-core-refactor architecture.

## 30-Second Pitch

This project replays raw Taobao `UserBehavior.csv` rows through Python into
Kafka using Avro and Schema Registry. A single Java Flink DataStream job
validates events, assigns event time, handles late data, writes raw history and
one-minute item aggregates and durable audits to ClickHouse, and materializes
per-user active-cart state in Apache Cassandra. PostgreSQL and Debezium provide
the full-profile broadcast-rule control plane, while Grafana is a ClickHouse
visualization client.

## 90-Second Pitch

The event grain is one raw user behavior. Python reads in bounded batches,
rejects malformed rows, creates deterministic event IDs, and publishes Avro
values keyed by user ID. Kafka preserves a user’s partition affinity. Flink
consumes specific Avro records, validates them, assigns event timestamps, and
emits five-second-bounded watermarks. Valid on-time events branch to a raw
ClickHouse sink and a one-minute event-time item aggregation. The mandatory
active-cart branch keys by user and stores item-level cart lifecycle in MapState;
cart is an upsert, buy is a delete, and stale events are rejected before
prepared CQL mutations reach Cassandra. The optional CDC branch broadcasts
versioned PostgreSQL rules through Debezium and Kafka. Core requires Cassandra;
only CDC and Grafana are full-profile additions. ClickHouse replacement tables
provide deduplicated query results without claiming cross-system exactly once.

## Five-Minute Walkthrough

1. Start at `README.md`, the blueprint, and the profile table. State that
   ClickHouse owns history/analytics and Cassandra owns active-cart lookup.
2. Open `producer/src/taobao_replay/contracts.py` and explain event grain,
   validation, timestamp conversion, and deterministic identity.
3. Open `reader.py` and `replay.py` to show bounded memory, source-order
   replay, invalid-row handling, and optional pacing.
4. Open `kafka.py` and `schemas/user-behavior-event.avsc`. Explain the user key,
   partition affinity, Avro value, registry subject, and idempotent producer
   settings.
5. Open `TaobaoStreamJob.java`. Identify source, validation side output,
   watermark assignment, durable audit sinks, raw sink, window metrics,
   mandatory Cassandra branch, full-profile broadcast branch, and checkpoint policy.
6. Open `ActiveCartProjector.java` and its tests. Demonstrate cart/upsert,
   buy/delete, repeated events, multiple items, user isolation, and stale-buy
   protection.
7. Open `infra/clickhouse/schema.sql` and `infra/cassandra/schema.cql` to show
   the ownership boundary and query shapes.
8. Open `CassandraSessionFactory.java` and `CassandraActiveCartSink.java` to
   explain SCB loading, token authentication, prepared statements, lifecycle,
   and the absence of the Astra Data API.
9. Open the CDC schema, connector JSON, and `CartAbandonmentRuleProcessor` to
   explain compacted configuration and broadcast state.
10. Finish with Compose/Terraform/scripts and the status reports. Distinguish
    static verification from real Astra, Kafka, Flink, recovery, and teardown
    evidence.

## Interview Questions

1. What is the event grain in this system?
2. Why is `user_id` the Kafka key?
3. What determines a Kafka partition number?
4. What does Schema Registry protect here?
5. Why use Avro instead of arbitrary JSON?
6. How is a deterministic `event_id` constructed?
7. Why does the replay engine preserve source order?
8. What does a five-second bounded watermark mean?
9. What happens to a row that is late?
10. Why are invalid and late events side outputs?
11. Why is the raw ClickHouse table separate from item metrics?
12. Why does the metrics key include replay run ID?
13. Why is the metrics window event-time based?
14. What does Flink `keyBy` guarantee for state locality?
15. Why is active-cart state keyed by user and mapped by item?
16. How does a buy event remove a cart item?
17. How does the projector prevent a stale cart from recreating a bought item?
18. What does row-level idempotence mean for Cassandra writes?
19. Why is Cassandra not used for full event history?
20. Why is the Astra Data API not used?
21. Why do `core` and `full` fail when Cassandra configuration is absent?
22. What is stored in Flink Broadcast State?
23. Why is the rules topic compacted?
24. How does a rule version avoid replacing a newer rule with an older one?
25. What is the difference between keyed state and broadcast state?
26. Where are event-time timers used?
27. What does checkpointing guarantee in this job?
28. How do at-least-once transport, idempotent Cassandra writes, and
    effectively-once ClickHouse queries differ from true exactly once?
29. Which dependencies are external to the Compose file?
30. What evidence is still required before saying Astra is deployed?
