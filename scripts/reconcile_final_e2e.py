#!/usr/bin/env python3
"""Independently compute canonical bounded results from the raw fixture."""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path

from taobao_replay.contracts import UserBehaviorEvent
from taobao_replay.reader import iter_event_batches

MAX_OUT_OF_ORDERNESS_MS = 5_000


def semantic_invalid_reason(event: UserBehaviorEvent) -> str | None:
    if min(event.user_id, event.item_id, event.category_id) <= 0:
        return "IDs must be positive"
    if event.event_time_ms < 0:
        return "event_time_ms must be non-negative"
    return None


def stable_digest(rows: Sequence[object]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def metric_rows(events: Sequence[UserBehaviorEvent]) -> list[dict[str, int]]:
    counters: dict[tuple[int, int, int], Counter[str]] = defaultdict(Counter)
    users: dict[tuple[int, int, int], set[int]] = defaultdict(set)
    for event in events:
        key = (
            event.event_time_ms // 60_000 * 60_000,
            event.item_id,
            event.category_id,
        )
        counters[key][event.behavior_type] += 1
        users[key].add(event.user_id)
    return [
        {
            "window_start": key[0],
            "item_id": key[1],
            "source_category_id": key[2],
            "pv_count": counts["pv"],
            "cart_count": counts["cart"],
            "fav_count": counts["fav"],
            "buy_count": counts["buy"],
            "unique_users": len(users[key]),
        }
        for key, counts in sorted(counters.items())
    ]


def build_expected(fixture: Path, run_ids: Sequence[str] | str) -> dict[str, object]:
    if isinstance(run_ids, str):
        run_ids = [run_ids]
    if not run_ids or any(not run_id.strip() for run_id in run_ids):
        raise ValueError("at least one non-blank replay run ID is required")

    attempts: list[UserBehaviorEvent] = []
    producer_rejected = 0
    fixture_source_rows: int | None = None
    for run_id in run_ids:
        attempt_rows = 0
        for batch in iter_event_batches(fixture, replay_run_id=run_id):
            attempts.extend(batch.events)
            producer_rejected += len(batch.issues)
            attempt_rows += batch.source_rows
        if fixture_source_rows is None:
            fixture_source_rows = attempt_rows
        elif fixture_source_rows != attempt_rows:
            raise AssertionError("fixture row count changed between replay attempts")

    invalid: list[UserBehaviorEvent] = []
    valid: list[UserBehaviorEvent] = []
    for event in attempts:
        (invalid if semantic_invalid_reason(event) else valid).append(event)

    seen_event_ids: set[str] = set()
    duplicates: list[UserBehaviorEvent] = []
    accepted_unique: list[UserBehaviorEvent] = []
    for event in valid:
        if event.event_id in seen_event_ids:
            duplicates.append(event)
        else:
            seen_event_ids.add(event.event_id)
            accepted_unique.append(event)

    maximum_timestamp: int | None = None
    on_time: list[UserBehaviorEvent] = []
    late: list[UserBehaviorEvent] = []
    for event in accepted_unique:
        maximum_timestamp = (
            event.event_time_ms
            if maximum_timestamp is None
            else max(maximum_timestamp, event.event_time_ms)
        )
        watermark = maximum_timestamp - MAX_OUT_OF_ORDERNESS_MS - 1
        (late if event.event_time_ms <= watermark else on_time).append(event)

    active_cart: dict[int, dict[int, UserBehaviorEvent]] = defaultdict(dict)
    latest_order: dict[tuple[int, int], tuple[int, int]] = {}
    for event in accepted_unique:
        if event.behavior_type not in {"cart", "buy"}:
            continue
        key = (event.user_id, event.item_id)
        order = (event.event_time_ms, event.source_sequence)
        if order <= latest_order.get(key, (-1, -1)):
            continue
        latest_order[key] = order
        if event.behavior_type == "cart":
            active_cart[event.user_id][event.item_id] = event
        else:
            active_cart[event.user_id].pop(event.item_id, None)

    metrics = metric_rows(on_time)
    item_500_metrics = [
        row for row in metrics if row["item_id"] == 500 and row["window_start"] == 1_511_658_000_000
    ]
    raw_business_rows = sorted(
        (
            {
                "event_id": event.event_id,
                "user_id": event.user_id,
                "item_id": event.item_id,
                "category_id": event.category_id,
                "behavior_type": event.behavior_type,
                "event_time": event.event_time_ms,
                "source_sequence": event.source_sequence,
            }
            for event in accepted_unique
        ),
        key=lambda row: row["event_id"],
    )
    quality_counts = {
        "INVALID": len(invalid),
        "DUPLICATE": len(duplicates),
        "LATE": len(late),
    }
    return {
        "fixture": fixture.as_posix(),
        "replay_run_ids": list(run_ids),
        "fixture_source_rows": fixture_source_rows or 0,
        "replay_attempts": len(run_ids),
        "attempted_source_rows": (fixture_source_rows or 0) * len(run_ids),
        "producer_rejected_rows": producer_rejected,
        "flink_decoded_rows": len(attempts),
        "invalid_events": len(invalid),
        "valid_events": len(valid),
        "duplicate_events": len(duplicates),
        "accepted_unique_events": len(accepted_unique),
        "late_for_aggregation_events": len(late),
        "on_time_events": len(on_time),
        "canonical_raw_events": len(accepted_unique),
        "canonical_raw_digest": stable_digest(raw_business_rows),
        "canonical_metric_rows": len(metrics),
        "canonical_metric_digest": stable_digest(metrics),
        "item_500_metric_values": item_500_metrics,
        "quality_counts": quality_counts,
        "user_100_active_cart_item_ids": sorted(active_cart[100]),
    }


def main() -> int:
    fixture = Path(os.environ.get("FIXTURE_PATH", "tests/fixtures/user_behavior_fixture.csv"))
    run_ids = [
        value.strip()
        for value in os.environ.get("REPLAY_RUN_IDS", "fixture-run-a,fixture-run-b").split(",")
        if value.strip()
    ]
    result = build_expected(fixture, run_ids)
    evidence_dir = Path(os.environ.get("EVIDENCE_DIR", "docs/evidence/latest"))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    output = evidence_dir / "expected-reconciliation.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
