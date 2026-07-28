from __future__ import annotations

import json
import time
import uuid
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer
from taobao_replay.kafka import kafka_security_options

from scripts.runtime_env import load_environment


def timestamp_millis(value: int | str) -> int:
    if isinstance(value, int):
        if value >= 100_000_000_000_000:
            return value // 1_000
        if value < 100_000_000_000:
            return value * 1_000
        return value
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1_000)


def normalized(after: dict[str, object]) -> dict[str, object]:
    return {
        "product_id": int(after["product_id"]),
        "category_id": int(after["category_id"]),
        "product_name": str(after["product_name"]),
        "price": str(after["price"]),
        "is_active": bool(after["is_active"]),
        "updated_at_ms": timestamp_millis(after["updated_at"]),
        "catalog_version": int(after["catalog_version"]),
    }


def main() -> int:
    environment = load_environment()
    profile = environment.get("RUNTIME_PROFILE", "local-smoke")
    bootstrap = (
        "localhost:9092" if profile == "local-smoke" else environment["KAFKA_BOOTSTRAP_SERVERS"]
    )
    config: dict[str, object] = {
        "bootstrap.servers": bootstrap,
        "group.id": f"catalog-wait-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    config.update(kafka_security_options(environment))
    expected_rows = json.loads(
        Path("tests/fixtures/product_catalog_expected.json").read_text(encoding="utf-8")
    )
    expected = {row["product_id"]: row for row in expected_rows}
    current: dict[int, dict[str, object]] = {}
    consumer = Consumer(config)
    consumer.subscribe([environment.get("KAFKA_CATALOG_TOPIC", "product-catalog-cdc")])
    deadline = time.monotonic() + 120
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(2)
            if message is None:
                continue
            if message.error():
                if message.error().retriable():
                    continue
                raise RuntimeError(str(message.error()))
            if message.value() is None:
                continue
            envelope = json.loads(message.value())
            if envelope.get("op") == "d" or not envelope.get("after"):
                continue
            row = normalized(envelope["after"])
            product_id = int(row["product_id"])
            previous = current.get(product_id)
            if previous is None or int(row["catalog_version"]) >= int(previous["catalog_version"]):
                current[product_id] = row
            if current == expected:
                print("Catalog CDC topic converged to the exact five-product fixture.")
                return 0
    finally:
        consumer.close()
    raise RuntimeError("catalog CDC topic did not converge within 120 seconds")


if __name__ == "__main__":
    raise SystemExit(main())
