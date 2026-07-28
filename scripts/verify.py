from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.runtime_env import load_environment

EXPECTED = Path("tests/fixtures/golden_outputs.json")
ENVIRONMENT = load_environment()


def clickhouse(sql: str) -> list[dict[str, object]]:
    endpoint = ENVIRONMENT.get("CLICKHOUSE_ENDPOINT", "http://localhost:8123")
    database = ENVIRONMENT.get("CLICKHOUSE_DATABASE", "taobao_behavior")
    user = ENVIRONMENT.get("CLICKHOUSE_USER", "default")
    password = ENVIRONMENT.get("CLICKHOUSE_PASSWORD", "local-clickhouse")
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
            "SELECT quality_type,event_id,replay_run_id,"
            "source_sequence,reason_code FROM stream_quality_events_canonical "
            "WHERE replay_run_id IN ('golden-a','golden-b') "
            "ORDER BY quality_type,replay_run_id,source_sequence"
        ),
        {"source_sequence"},
    )
    actual = {
        "canonical_raw": raw,
        "metrics": metrics,
        "quality": quality,
    }
    if not args.snapshot_only:
        assert raw == expected["canonical_raw"]
        assert metrics == expected["metrics"]
        expected_quality = [
            {
                key: row[key]
                for key in (
                    "quality_type",
                    "event_id",
                    "replay_run_id",
                    "source_sequence",
                    "reason_code",
                )
            }
            for row in expected["quality"]
        ]
        expected_quality.sort(
            key=lambda row: (
                row["quality_type"],
                row["replay_run_id"],
                row["source_sequence"],
            )
        )
        assert quality == expected_quality
    if args.snapshot:
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.snapshot.write_text(json.dumps(actual, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "rows": len(raw),
                "metrics": len(metrics),
                "quality": len(quality),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
