from __future__ import annotations

import argparse
import json
import os
from hashlib import sha256
from pathlib import Path

from taobao_replay.clickhouse import ClickHouseHttpClient


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Verify canonical ClickHouse results for a fresh bounded-demo database"
    )
    result.add_argument("--run-id", action="append", required=True)
    result.add_argument(
        "--expected-file",
        type=Path,
        default=Path(os.getenv("EVIDENCE_DIR", "docs/evidence/latest"))
        / "expected-reconciliation.json",
    )
    result.add_argument("--endpoint", default=os.getenv("CLICKHOUSE_ENDPOINT"))
    result.add_argument("--user", default=os.getenv("CLICKHOUSE_USER", "default"))
    result.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    result.add_argument("--database", default=os.getenv("CLICKHOUSE_DATABASE", "taobao_behavior"))
    return result


def sql_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def query_rows(client: ClickHouseHttpClient, sql: str) -> list[dict[str, object]]:
    result = client.execute(sql, json_result=True)
    assert result is not None
    return list(result["data"])


def stable_digest(rows: list[dict[str, object]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def integerized(rows: list[dict[str, object]], integer_fields: set[str]) -> list[dict[str, object]]:
    return [
        {key: int(value) if key in integer_fields else value for key, value in row.items()}
        for row in rows
    ]


def require_equal(label: str, expected: object, actual: object) -> None:
    if expected != actual:
        raise SystemExit(f"{label} mismatch: expected {expected!r}, got {actual!r}")


def main() -> int:
    args = parser().parse_args()
    if not args.endpoint:
        parser().error("set CLICKHOUSE_ENDPOINT or pass --endpoint")
    expected = json.loads(args.expected_file.read_text(encoding="utf-8"))
    require_equal("replay run IDs", expected["replay_run_ids"], args.run_id)

    client = ClickHouseHttpClient(args.endpoint, args.user, args.password, args.database)
    raw_rows = integerized(
        query_rows(
            client,
            "SELECT event_id, user_id, item_id, category_id, behavior_type, "
            "toUnixTimestamp64Milli(event_time) AS event_time, source_sequence "
            "FROM raw_behavior_events_canonical ORDER BY event_id",
        ),
        {"user_id", "item_id", "category_id", "event_time", "source_sequence"},
    )
    require_equal("canonical raw count", expected["canonical_raw_events"], len(raw_rows))
    require_equal("canonical raw digest", expected["canonical_raw_digest"], stable_digest(raw_rows))

    metric_rows = integerized(
        query_rows(
            client,
            "SELECT toUnixTimestamp64Milli(window_start) AS window_start, "
            "item_id, source_category_id, pv_count, cart_count, fav_count, buy_count, unique_users "
            "FROM item_metrics_1m_canonical "
            "ORDER BY window_start, item_id, source_category_id",
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
    require_equal("canonical metric rows", expected["canonical_metric_rows"], len(metric_rows))
    require_equal(
        "canonical metric digest",
        expected["canonical_metric_digest"],
        stable_digest(metric_rows),
    )

    run_filter = ", ".join(sql_literal(run_id) for run_id in args.run_id)
    quality_rows = query_rows(
        client,
        "SELECT quality_type, count() AS quality_count "
        "FROM stream_quality_events_canonical "
        f"WHERE replay_run_id IN ({run_filter}) "
        "GROUP BY quality_type ORDER BY quality_type",
    )
    actual_quality = {str(row["quality_type"]): int(row["quality_count"]) for row in quality_rows}
    expected_quality = {key: value for key, value in expected["quality_counts"].items() if value}
    require_equal("quality counts", expected_quality, actual_quality)

    print(
        json.dumps(
            {
                "canonical_raw_events": len(raw_rows),
                "canonical_raw_digest": stable_digest(raw_rows),
                "canonical_metric_rows": len(metric_rows),
                "canonical_metric_digest": stable_digest(metric_rows),
                "quality_counts": actual_quality,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
