from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from taobao_replay.reader import iter_event_batches

EXPECTED = Path("tests/fixtures/golden_outputs.json")
EXPECTED_CATALOG = Path("tests/fixtures/product_catalog_expected.json")
SOURCE_FIXTURE = Path("tests/fixtures/user_behavior_fixture.csv")
REPLAY_RUN_IDS = ("golden-a", "golden-b")


def require_equal(label: str, actual: object, expected: object) -> None:
    if actual == expected:
        return
    diagnostic = {
        "check": label,
        "expected": expected,
        "actual": actual,
    }
    raise RuntimeError(
        "verification mismatch:\n" + json.dumps(diagnostic, indent=2, sort_keys=True)
    )


def clickhouse(sql: str) -> list[dict[str, object]]:
    endpoint = os.getenv("CLICKHOUSE_ENDPOINT", "http://localhost:8123")
    database = os.getenv("CLICKHOUSE_DATABASE", "taobao_behavior")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "local-clickhouse")
    url = f"{endpoint}?{urlencode({'database': database})}"
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = Request(
        url,
        data=f"{sql}\nFORMAT JSON".encode(),
        headers={"Authorization": f"Basic {token}"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310
        return json.load(response)["data"]


def integerize(rows: list[dict[str, object]], fields: set[str]) -> list[dict[str, object]]:
    return [
        {key: int(value) if key in fields else value for key, value in row.items()} for row in rows
    ]


def redis_hash_key(key: str) -> tuple[dict[str, str], int]:
    """Read one Redis hash and TTL through the running Compose service."""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "infra/docker-compose.yml",
            "exec",
            "-T",
            "redis",
            "redis-cli",
            "--json",
            "HGETALL",
            key,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    values = json.loads(result.stdout)
    cart = values if isinstance(values, dict) else dict(zip(values[::2], values[1::2], strict=True))
    ttl = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "infra/docker-compose.yml",
            "exec",
            "-T",
            "redis",
            "redis-cli",
            "TTL",
            key,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return cart, int(ttl.stdout.strip())


def redis_cart(user_id: str) -> tuple[dict[str, str], int]:
    return redis_hash_key(f"taobao:active_cart:{{{user_id}}}")


def redis_features(user_id: str) -> tuple[dict[str, str], int]:
    return redis_hash_key(f"taobao:features:user:{{{user_id}}}")


def reconcile_accounting(
    raw: list[dict[str, object]], quality: list[dict[str, object]]
) -> dict[str, int]:
    """Derive duplicate volume without requiring per-duplicate audit records."""
    decoded = producer_rejected = source_rows = 0
    for run_id in REPLAY_RUN_IDS:
        for batch in iter_event_batches(SOURCE_FIXTURE, replay_run_id=run_id, batch_size=100):
            source_rows += batch.source_rows
            decoded += len(batch.events)
            producer_rejected += len(batch.issues)

    invalid = sum(row["quality_type"] == "INVALID" for row in quality)
    late = sum(row["quality_type"] == "LATE" for row in quality)
    valid = decoded - invalid
    accepted_unique = len(raw)
    duplicate = valid - accepted_unique
    if duplicate < 0 or late > accepted_unique:
        raise RuntimeError(
            "invalid reconciliation: canonical and quality counts exceed fixture input"
        )
    return {
        "replay_attempts": len(REPLAY_RUN_IDS),
        "source_rows": source_rows,
        "decoded": decoded,
        "invalid": invalid,
        "valid": valid,
        "duplicate": duplicate,
        "accepted_unique": accepted_unique,
        "on_time": accepted_unique - late,
        "late_for_aggregation": late,
        "canonical_raw": accepted_unique,
        "metrics_input": accepted_unique - late,
        "producer_rejected": producer_rejected,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--snapshot-only", action="store_true")
    parser.add_argument("--with-catalog", action="store_true")
    args = parser.parse_args()
    expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
    raw = integerize(
        clickhouse(
            "SELECT event_id,user_id,item_id,category_id,behavior_type,"
            "toUnixTimestamp64Milli(event_time) event_time_ms,source_sequence "
            "FROM raw_behavior_events_canonical ORDER BY source_sequence"
        ),
        {"user_id", "item_id", "category_id", "event_time_ms", "source_sequence"},
    )
    metrics = integerize(
        clickhouse(
            "SELECT toUnixTimestamp64Milli(window_start) window_start,item_id,"
            "source_category_id,pv_count,cart_count,fav_count,buy_count,unique_users "
            "FROM item_metrics_1m_canonical ORDER BY window_start,item_id,source_category_id"
        ),
        {
            "window_start",
            "item_id",
            "source_category_id",
            "pv_count",
            "cart_count",
            "fav_count",
            "buy_count",
            "unique_users",
        },
    )
    user_features = integerize(
        clickhouse(
            "SELECT feature_id,toUnixTimestamp64Milli(window_start) window_start,"
            "toUnixTimestamp64Milli(window_end) window_end,user_id,event_count,pv_count,"
            "cart_count,fav_count,buy_count,distinct_items,feature_version "
            "FROM user_features_1m_canonical ORDER BY window_start,user_id"
        ),
        {
            "window_start",
            "window_end",
            "user_id",
            "event_count",
            "pv_count",
            "cart_count",
            "fav_count",
            "buy_count",
            "distinct_items",
            "feature_version",
        },
    )
    quality = integerize(
        clickhouse(
            "SELECT quality_event_id,quality_type,event_id,replay_run_id,"
            "source_sequence,reason_code FROM stream_quality_events_canonical "
            "WHERE replay_run_id IN ('golden-a','golden-b') ORDER BY quality_event_id"
        ),
        {"source_sequence"},
    )
    carts = {}
    cart_ttls = {}
    for user_id in expected["redis_active_carts"]:
        carts[user_id], cart_ttls[user_id] = redis_cart(user_id)
    online_features = {}
    feature_ttls = {}
    for user_id in expected["redis_user_features"]:
        online_features[user_id], feature_ttls[user_id] = redis_features(user_id)
    accounting = reconcile_accounting(raw, quality)
    actual = {
        "accounting": accounting,
        "canonical_raw": raw,
        "metrics": metrics,
        "user_features": user_features,
        "quality": quality,
        "redis_active_carts": carts,
        "redis_user_features": online_features,
    }
    if args.with_catalog:
        catalog = integerize(
            clickhouse(
                "SELECT product_id,category_id,product_name,toString(price) price,"
                "is_active,catalog_version FROM product_catalog_current_canonical "
                "ORDER BY product_id"
            ),
            {"product_id", "category_id", "catalog_version"},
        )
        actual["product_catalog"] = catalog
        if not args.snapshot_only:
            require_equal(
                "product_catalog",
                catalog,
                json.loads(EXPECTED_CATALOG.read_text(encoding="utf-8")),
            )
    if not args.snapshot_only:
        require_equal("accounting", accounting, expected["accounting"])
        require_equal("canonical_raw", raw, expected["canonical_raw"])
        require_equal("metrics", metrics, expected["metrics"])
        require_equal("user_features", user_features, expected["user_features"])
        require_equal(
            "quality",
            quality,
            sorted(expected["quality"], key=lambda row: row["quality_event_id"]),
        )
        require_equal("redis_active_carts", carts, expected["redis_active_carts"])
        require_equal(
            "redis_user_features",
            online_features,
            expected["redis_user_features"],
        )
    ttl_limit = int(os.getenv("REDIS_CART_TTL_SECONDS", "604800"))
    invalid_ttls = {
        user_id: ttl
        for user_id, ttl in cart_ttls.items()
        if not (ttl == -2 or 0 < ttl <= ttl_limit)
    }
    if invalid_ttls:
        raise RuntimeError(
            f"Redis cart TTL must be absent (-2) or in (0, {ttl_limit}], got {invalid_ttls}"
        )
    feature_ttl_limit = int(os.getenv("REDIS_FEATURE_TTL_SECONDS", "86400"))
    invalid_feature_ttls = {
        user_id: ttl for user_id, ttl in feature_ttls.items() if not (0 < ttl <= feature_ttl_limit)
    }
    if invalid_feature_ttls:
        raise RuntimeError(
            f"Redis feature TTL must be in (0, {feature_ttl_limit}], got {invalid_feature_ttls}"
        )
    if args.snapshot:
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.snapshot.write_text(json.dumps(actual, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "accounting": accounting,
                "rows": len(raw),
                "metrics": len(metrics),
                "user_features": len(user_features),
                "quality": len(quality),
                "cart_ttls": cart_ttls,
                "feature_ttls": feature_ttls,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
