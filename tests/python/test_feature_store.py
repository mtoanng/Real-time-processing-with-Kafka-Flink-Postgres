from __future__ import annotations

import unittest

from taobao_serving.redis_features import apply_feature_snapshot, feature_key


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, dict[str, str]] = {}
        self.ttls: dict[str, int] = {}

    def eval(self, _script: str, key_count: int, key: str, *args: object) -> int:
        if key_count != 1:
            raise AssertionError("feature script must operate on exactly one key")
        version = int(args[0])
        current = int(self.values.get(key, {}).get("feature_version", "-1"))
        if current >= version:
            return 0
        fields = (
            "feature_version",
            "feature_id",
            "window_start",
            "window_end",
            "event_count",
            "pv_count",
            "cart_count",
            "fav_count",
            "buy_count",
            "distinct_items",
        )
        self.values[key] = {
            field: str(value) for field, value in zip(fields, args[:10], strict=True)
        }
        self.ttls[key] = int(args[10])
        return 1


def snapshot(version: int = 1_511_658_060_000, *, event_count: int = 5):
    return {
        "feature_id": "feature-1",
        "window_start": "2017-11-26 02:20:00.000",
        "window_end": "2017-11-26 02:21:00.000",
        "user_id": 1,
        "event_count": event_count,
        "pv_count": 1,
        "cart_count": 3,
        "fav_count": 0,
        "buy_count": 1,
        "distinct_items": 2,
        "feature_version": version,
        "record_version": version,
    }


class RedisFeatureStoreTests(unittest.TestCase):
    def test_newer_snapshot_is_applied_with_ttl(self) -> None:
        redis = FakeRedis()
        self.assertTrue(apply_feature_snapshot(redis, snapshot(), 86_400))
        key = feature_key(1)
        self.assertEqual("5", redis.values[key]["event_count"])
        self.assertEqual("2", redis.values[key]["distinct_items"])
        self.assertEqual(86_400, redis.ttls[key])

    def test_duplicate_and_stale_snapshots_cannot_overwrite_latest(self) -> None:
        redis = FakeRedis()
        current = snapshot()
        self.assertTrue(apply_feature_snapshot(redis, current, 86_400))
        self.assertFalse(apply_feature_snapshot(redis, current, 86_400))
        self.assertFalse(
            apply_feature_snapshot(
                redis,
                snapshot(current["feature_version"] - 60_000),
                86_400,
            )
        )
        self.assertEqual(
            str(current["feature_version"]),
            redis.values[feature_key(1)]["feature_version"],
        )

    def test_invalid_feature_contract_fails_before_redis_io(self) -> None:
        redis = FakeRedis()
        with self.assertRaisesRegex(ValueError, "sum of behavior counts"):
            apply_feature_snapshot(redis, snapshot(event_count=99), 86_400)
        with self.assertRaisesRegex(ValueError, "TTL"):
            apply_feature_snapshot(redis, snapshot(), 0)
        self.assertEqual({}, redis.values)

    def test_key_uses_user_hash_tag(self) -> None:
        self.assertEqual("taobao:features:user:{42}", feature_key(42))
        with self.assertRaisesRegex(ValueError, "user_id"):
            feature_key(0)


if __name__ == "__main__":
    unittest.main()
