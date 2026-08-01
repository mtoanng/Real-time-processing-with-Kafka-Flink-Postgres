"""Materialize compacted Flink cart mutations into a rebuildable Redis view.

This adapter owns Redis I/O only.  Flink owns cart/buy/stale-event business
semantics, and Kafka retains the mutation log needed to rebuild Redis.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping

from taobao_replay.kafka import kafka_security_options


def apply_mutation(redis_client: object, mutation: Mapping[str, object], ttl_seconds: int) -> None:
    """Apply one deterministic UPSERT or DELETE to a user's Redis cart hash."""
    operation = str(mutation["operation"])
    user_id = int(mutation["user_id"])
    item_id = int(mutation["item_id"])
    key = f"taobao:active_cart:{{{user_id}}}"
    if operation == "UPSERT":
        value = "|".join(
            (
                str(int(mutation["category_id"])),
                str(int(mutation["added_at_ms"])),
                str(int(mutation["last_updated_at_ms"])),
            )
        )
        redis_client.hset(key, str(item_id), value)
        redis_client.expire(key, ttl_seconds)
    elif operation == "DELETE":
        redis_client.hdel(key, str(item_id))
    else:
        raise ValueError(f"unsupported cart operation: {operation}")


def main() -> None:
    """Consume mutations synchronously and commit Kafka offsets after Redis succeeds."""
    from confluent_kafka import Consumer
    from redis import Redis

    ttl_seconds = int(os.getenv("REDIS_CART_TTL_SECONDS", "604800"))
    if ttl_seconds <= 0:
        raise ValueError("REDIS_CART_TTL_SECONDS must be greater than zero")
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
        "group.id": os.getenv("REDIS_CART_CONSUMER_GROUP", "redis-cart-materializer-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer_config.update(kafka_security_options(os.environ))
    consumer = Consumer(consumer_config)
    consumer.subscribe([os.getenv("KAFKA_CART_MUTATION_TOPIC", "taobao-active-cart-mutations")])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"cart mutation consumer failed: {message.error()}")
            if message.value() is None:
                continue
            apply_mutation(redis_client, json.loads(message.value()), ttl_seconds)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        redis_client.close()


if __name__ == "__main__":
    main()
