#!/usr/bin/env python3
"""Capture or compare canonical business results for recovery experiments."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from taobao_replay.clickhouse import ClickHouseHttpClient

RAW_SQL = (
    "SELECT event_id, user_id, item_id, category_id, behavior_type, "
    "toUnixTimestamp64Milli(event_time) AS event_time, source_sequence "
    "FROM raw_behavior_events_canonical ORDER BY event_id"
)
METRICS_SQL = (
    "SELECT toUnixTimestamp64Milli(window_start) AS window_start, "
    "item_id, source_category_id, pv_count, cart_count, fav_count, buy_count, unique_users "
    "FROM item_metrics_1m_canonical ORDER BY window_start, item_id, source_category_id"
)


def compare_snapshots(baseline: dict[str, object], recovered: dict[str, object]) -> None:
    for key in ("canonical_raw", "canonical_metrics", "active_cart"):
        if key not in baseline and key not in recovered:
            continue
        if baseline.get(key) != recovered.get(key):
            raise ValueError(
                f"{key} differs: baseline={baseline.get(key)!r}, recovered={recovered.get(key)!r}"
            )


def query_rows(client: ClickHouseHttpClient, sql: str) -> list[dict[str, object]]:
    result = client.execute(sql, json_result=True)
    assert result is not None
    return list(result["data"])


def normalized_cart(path: Path | None) -> list[str] | None:
    if path is None:
        return None
    return sorted(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith("user_id=")
    )


def capture(args: argparse.Namespace) -> int:
    if not args.endpoint:
        raise SystemExit("CLICKHOUSE_ENDPOINT or --endpoint is required")
    client = ClickHouseHttpClient(args.endpoint, args.user, args.password, args.database)
    snapshot: dict[str, object] = {
        "canonical_raw": query_rows(client, RAW_SQL),
        "canonical_metrics": query_rows(client, METRICS_SQL),
    }
    cart = normalized_cart(args.active_cart_file)
    if cart is not None:
        snapshot["active_cart"] = cart
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"captured canonical snapshot: {args.output}")
    return 0


def compare(args: argparse.Namespace) -> int:
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    recovered = json.loads(args.recovered.read_text(encoding="utf-8"))
    try:
        compare_snapshots(baseline, recovered)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print("canonical recovery comparison passed")
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    commands = result.add_subparsers(dest="command", required=True)
    capture_parser = commands.add_parser("capture")
    capture_parser.add_argument("--output", type=Path, required=True)
    capture_parser.add_argument("--active-cart-file", type=Path)
    capture_parser.add_argument("--endpoint", default=os.getenv("CLICKHOUSE_ENDPOINT"))
    capture_parser.add_argument("--user", default=os.getenv("CLICKHOUSE_USER", "default"))
    capture_parser.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    capture_parser.add_argument(
        "--database", default=os.getenv("CLICKHOUSE_DATABASE", "taobao_behavior")
    )
    capture_parser.set_defaults(handler=capture)
    compare_parser = commands.add_parser("compare")
    compare_parser.add_argument("--baseline", type=Path, required=True)
    compare_parser.add_argument("--recovered", type=Path, required=True)
    compare_parser.set_defaults(handler=compare)
    return result


def main() -> int:
    args = parser().parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
