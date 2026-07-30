"""Read adapter for rebuildable Redis cart state and ClickHouse analytics."""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class ServingStore:
    """Expose serving queries without making Redis or ClickHouse business sources."""

    def __init__(self, redis_client: object, environment: Mapping[str, str]) -> None:
        self._redis = redis_client
        self._environment = environment

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> ServingStore:
        """Create a Redis client from deployment configuration."""
        from redis import Redis

        active = os.environ if environment is None else environment
        client = Redis(
            host=active.get("REDIS_HOST", "localhost"),
            port=int(active.get("REDIS_PORT", "6379")),
            username=active.get("REDIS_USERNAME") or None,
            password=active.get("REDIS_PASSWORD") or None,
            ssl=active.get("REDIS_TLS", "false").lower() == "true",
            decode_responses=True,
        )
        return cls(client, active)

    def close(self) -> None:
        """Release the Redis client held by this process."""
        self._redis.close()

    def cart(self, user_id: int) -> dict[str, object]:
        """Read the current clickstream-derived cart projection for one user."""
        key = f"taobao:active_cart:{{{user_id}}}"
        values = self._redis.hgetall(key)
        items = []
        for item_id, value in sorted(values.items(), key=lambda pair: int(pair[0])):
            category_id, added_at, last_updated = (int(part) for part in value.split("|"))
            items.append(
                {
                    "product_id": int(item_id),
                    "category_id": category_id,
                    "added_at": added_at,
                    "last_updated_at": last_updated,
                }
            )
        return {"user_id": user_id, "items": items, "ttl_seconds": int(self._redis.ttl(key))}

    def trending(self, minutes: int, as_of_ms: int | None) -> dict[str, object]:
        """Join historical metrics with current catalog metadata at query time.

        This is late-binding current-state enrichment, not a historical as-of
        catalog join. ``as_of_ms`` supports the 2017 bounded fixture.
        """
        as_of = "now64(3)" if as_of_ms is None else f"fromUnixTimestamp64Milli({as_of_ms})"
        sql = f"""
WITH {as_of} AS as_of
SELECT
    m.item_id AS product_id,
    any(p.product_name) AS product_name,
    any(toString(p.price)) AS current_price,
    sum(m.pv_count) AS views,
    sum(m.cart_count) AS carts,
    sum(m.buy_count) AS purchases
FROM item_metrics_1m_canonical AS m
INNER JOIN product_catalog_current_canonical AS p
    ON m.item_id = p.product_id
WHERE m.window_start >= as_of - toIntervalMinute({minutes})
  AND m.window_start < as_of
  AND p.is_active
GROUP BY m.item_id
ORDER BY purchases DESC, carts DESC, views DESC, product_id ASC
LIMIT 20
"""
        products = self._clickhouse(sql)
        integer_fields = {"product_id", "views", "carts", "purchases"}
        normalized = [
            {
                key: int(value)
                if key in integer_fields
                else float(value)
                if key == "current_price"
                else value
                for key, value in row.items()
            }
            for row in products
        ]
        return {"window_minutes": minutes, "as_of_ms": as_of_ms, "products": normalized}

    def _clickhouse(self, sql: str) -> list[dict[str, object]]:
        """Execute an internal ClickHouse JSON query using configured credentials."""
        endpoint = self._environment.get("CLICKHOUSE_ENDPOINT", "http://localhost:8123")
        database = self._environment.get("CLICKHOUSE_DATABASE", "taobao_behavior")
        user = self._environment.get("CLICKHOUSE_USER", "default")
        password = self._environment.get("CLICKHOUSE_PASSWORD", "")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        request = Request(
            f"{endpoint}?{urlencode({'database': database})}",
            data=f"{sql}\nFORMAT JSON".encode(),
            headers={"Authorization": f"Basic {token}"},
            method="POST",
        )
        with urlopen(request, timeout=10) as response:  # noqa: S310
            return json.load(response)["data"]
