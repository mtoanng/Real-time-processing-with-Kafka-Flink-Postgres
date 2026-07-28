from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

EXPECTED = Path("tests/fixtures/golden_outputs.json")


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


def redis_hash(user_id: str) -> tuple[dict[str, str], int]:
    key = f"taobao:active_cart:{{{user_id}}}"
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--snapshot-only", action="store_true")
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
    quality = integerize(
        clickhouse(
            "SELECT quality_event_id,quality_type,event_id,replay_run_id,"
            "source_sequence,reason_code FROM stream_quality_events_canonical "
            "WHERE replay_run_id IN ('golden-a','golden-b') ORDER BY quality_event_id"
        ),
        {"source_sequence"},
    )
    carts = {}
    ttls = {}
    for user_id in expected["redis_active_carts"]:
        carts[user_id], ttls[user_id] = redis_hash(user_id)
    actual = {
        "canonical_raw": raw,
        "metrics": metrics,
        "quality": quality,
        "redis_active_carts": carts,
    }
    if not args.snapshot_only:
        assert raw == expected["canonical_raw"]
        assert metrics == expected["metrics"]
        assert quality == sorted(expected["quality"], key=lambda row: row["quality_event_id"])
        assert carts == expected["redis_active_carts"]
    ttl_limit = int(os.getenv("REDIS_CART_TTL_SECONDS", "604800"))
    assert all(ttl == -2 or 0 < ttl <= ttl_limit for ttl in ttls.values())
    if args.snapshot:
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.snapshot.write_text(json.dumps(actual, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {"rows": len(raw), "metrics": len(metrics), "quality": len(quality), "ttls": ttls}
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
