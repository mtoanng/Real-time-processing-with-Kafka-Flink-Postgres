"""Materialize the latest user feature snapshot into Redis.

Flink SQL emits append-only, event-time feature history. ClickHouse retains
that history for offline training, while this adapter atomically keeps only the
greatest ``feature_version`` for each user and applies a freshness TTL. Kafka
offsets are committed only after Redis accepts or safely rejects the snapshot.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping

from taobao_events.kafka import kafka_security_options

_APPLY_FEATURE = """
local current = redis.call('HGET', KEYS[1], 'feature_version')
if current and tonumber(current) >= tonumber(ARGV[1]) then
    return 0
end
redis.call('HSET', KEYS[1],
    'feature_version', ARGV[1],
    'feature_id', ARGV[2],
    'window_start', ARGV[3],
    'window_end', ARGV[4],
    'event_count', ARGV[5],
    'pv_count', ARGV[6],
    'cart_count', ARGV[7],
    'fav_count', ARGV[8],
    'buy_count', ARGV[9],
    'distinct_items', ARGV[10])
redis.call('EXPIRE', KEYS[1], ARGV[11])
return 1
"""

_COUNT_FIELDS = (
    "event_count",
    "pv_count",
    "cart_count",
    "fav_count",
    "buy_count",
    "distinct_items",
)


def feature_key(user_id: int) -> str:
    """Return the cluster-safe Redis key for one user's online features."""
    if user_id <= 0:
        raise ValueError("user_id must be greater than zero")
    return f"taobao:features:user:{{{user_id}}}"


def apply_feature_snapshot(
    redis_client: object,
    snapshot: Mapping[str, object],
    ttl_seconds: int,
) -> bool:
    """Atomically apply a newer snapshot and reject duplicates or stale versions."""
    if ttl_seconds <= 0:
        raise ValueError("feature TTL must be greater than zero")
    user_id = int(snapshot["user_id"])
    version = int(snapshot["feature_version"])
    if version <= 0:
        raise ValueError("feature_version must be greater than zero")
    feature_id = str(snapshot["feature_id"]).strip()
    if not feature_id:
        raise ValueError("feature_id must not be blank")
    counts = {field: int(snapshot[field]) for field in _COUNT_FIELDS}
    if any(value < 0 for value in counts.values()):
        raise ValueError("feature counts must not be negative")
    behavior_total = sum(
        counts[field] for field in ("pv_count", "cart_count", "fav_count", "buy_count")
    )
    if counts["event_count"] != behavior_total:
        raise ValueError("event_count must equal the sum of behavior counts")
    if counts["distinct_items"] > counts["event_count"]:
        raise ValueError("distinct_items cannot exceed event_count")

    result = redis_client.eval(
        _APPLY_FEATURE,
        1,
        feature_key(user_id),
        version,
        feature_id,
        str(snapshot["window_start"]),
        str(snapshot["window_end"]),
        counts["event_count"],
        counts["pv_count"],
        counts["cart_count"],
        counts["fav_count"],
        counts["buy_count"],
        counts["distinct_items"],
        ttl_seconds,
    )
    return int(result) == 1


def main() -> None:
    """Consume feature snapshots and commit Kafka offsets after atomic Redis I/O."""
    from confluent_kafka import Consumer
    from redis import Redis

    ttl_seconds = int(os.getenv("REDIS_FEATURE_TTL_SECONDS", "86400"))
    if ttl_seconds <= 0:
        raise ValueError("REDIS_FEATURE_TTL_SECONDS must be greater than zero")
    redis_client = Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        username=os.getenv("REDIS_USERNAME") or None,
        password=os.getenv("REDIS_PASSWORD") or None,
        ssl=os.getenv("REDIS_TLS", "false").lower() == "true",
        decode_responses=True,
    )
    consumer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "group.id": os.getenv("REDIS_FEATURE_CONSUMER_GROUP", "redis-feature-materializer-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer_config.update(kafka_security_options(os.environ))
    consumer = Consumer(consumer_config)
    consumer.subscribe([os.getenv("KAFKA_USER_FEATURES_TOPIC", "taobao-user-features-1m")])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"feature consumer failed: {message.error()}")
            if message.value() is None:
                consumer.commit(message=message, asynchronous=False)
                continue
            apply_feature_snapshot(redis_client, json.loads(message.value()), ttl_seconds)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        redis_client.close()


if __name__ == "__main__":
    main()
