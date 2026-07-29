from __future__ import annotations

import json
import unittest
from pathlib import Path

from taobao_api.serving import ServingStore

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = json.loads((ROOT / "tests/fixtures/serving_expected.json").read_text(encoding="utf-8"))


class FakeRedis:
    def hgetall(self, _key: str) -> dict[str, str]:
        return {"101": "11|1511658004000|1511658005000"}

    def ttl(self, _key: str) -> int:
        return 86_342


class FixtureServingStore(ServingStore):
    def __init__(self) -> None:
        super().__init__(FakeRedis(), {})
        self.sql = ""

    def _clickhouse(self, sql: str) -> list[dict[str, object]]:
        self.sql = sql
        return [
            {
                "product_id": "100",
                "product_name": "Wireless Headphones Pro",
                "current_price": "44.99",
                "views": "2",
                "carts": "1",
                "purchases": "1",
            },
            {
                "product_id": "102",
                "product_name": "Mechanical Keyboard",
                "current_price": "79.00",
                "views": "0",
                "carts": "1",
                "purchases": "1",
            },
            {
                "product_id": "104",
                "product_name": "Action Camera",
                "current_price": "129.00",
                "views": "1",
                "carts": "0",
                "purchases": "0",
            },
        ]


class ServingContractTests(unittest.TestCase):
    def test_cart_is_the_exact_redis_logical_state_with_ttl(self) -> None:
        actual = FixtureServingStore().cart(1)
        self.assertEqual(EXPECTED["cart"], {key: actual[key] for key in ("user_id", "items")})
        self.assertEqual(86_342, actual["ttl_seconds"])

    def test_trending_is_metrics_joined_with_current_catalog(self) -> None:
        store = FixtureServingStore()
        actual = store.trending(15, 1_511_658_120_000)
        self.assertEqual(EXPECTED["trending"], actual)
        self.assertIn("item_metrics_1m_canonical", store.sql)
        self.assertIn("product_catalog_current_canonical", store.sql)
        self.assertIn("fromUnixTimestamp64Milli(1511658120000)", store.sql)
        self.assertIn("p.is_active", store.sql)
