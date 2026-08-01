from __future__ import annotations

import hashlib
import json
import unittest
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from taobao_replay.reader import iter_event_batches

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/user_behavior_fixture.csv"
EXPECTED = ROOT / "tests/fixtures/golden_outputs.json"
RUN_IDS = ("golden-a", "golden-b")
MAX_OUT_OF_ORDERNESS_MS = 5_000


class GoldenContractTests(unittest.TestCase):
    def test_exact_outputs_and_accounting(self) -> None:
        expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
        attempts = [
            event
            for run_id in RUN_IDS
            for batch in iter_event_batches(FIXTURE, replay_run_id=run_id, batch_size=4)
            for event in batch.events
        ]
        invalid = [
            event for event in attempts if min(event.user_id, event.item_id, event.category_id) <= 0
        ]
        valid = [event for event in attempts if event not in invalid]

        seen: set[str] = set()
        accepted = []
        duplicates = []
        for event in valid:
            (duplicates if event.event_id in seen else accepted).append(event)
            seen.add(event.event_id)

        maximum = -(2**63)
        on_time = []
        late = []
        for event in accepted:
            maximum = max(maximum, event.event_time_ms)
            watermark = maximum - MAX_OUT_OF_ORDERNESS_MS
            (late if event.event_time_ms <= watermark else on_time).append(event)

        raw = [
            {
                "event_id": event.event_id,
                "user_id": event.user_id,
                "item_id": event.item_id,
                "category_id": event.category_id,
                "behavior_type": event.behavior_type,
                "event_time_ms": event.event_time_ms,
                "source_sequence": event.source_sequence,
            }
            for event in accepted
        ]

        counts: dict[tuple[int, int, int], Counter[str]] = defaultdict(Counter)
        users: dict[tuple[int, int, int], set[int]] = defaultdict(set)
        final_watermark = maximum - MAX_OUT_OF_ORDERNESS_MS
        closed_window_events = [
            event
            for event in on_time
            if (event.event_time_ms // 60_000 * 60_000) + 60_000 <= final_watermark
        ]
        for event in closed_window_events:
            key = (event.event_time_ms // 60_000 * 60_000, event.item_id, event.category_id)
            counts[key][event.behavior_type] += 1
            users[key].add(event.user_id)
        metrics = [
            {
                "window_start": key[0],
                "item_id": key[1],
                "source_category_id": key[2],
                "pv_count": values["pv"],
                "cart_count": values["cart"],
                "fav_count": values["fav"],
                "buy_count": values["buy"],
                "unique_users": len(users[key]),
            }
            for key, values in sorted(counts.items())
        ]

        feature_counts: dict[tuple[int, int], Counter[str]] = defaultdict(Counter)
        feature_items: dict[tuple[int, int], set[int]] = defaultdict(set)
        for event in closed_window_events:
            key = (event.event_time_ms // 60_000 * 60_000, event.user_id)
            feature_counts[key][event.behavior_type] += 1
            feature_items[key].add(event.item_id)
        user_features = []
        for key, values in sorted(feature_counts.items()):
            window_start, user_id = key
            window_end = window_start + 60_000
            label = datetime.fromtimestamp(window_start / 1000, UTC).strftime(
                "%Y-%m-%d %H:%M:%S.000"
            )
            user_features.append(
                {
                    "feature_id": hashlib.sha256(
                        f"USER_FEATURES_1M|{user_id}|{label}".encode()
                    ).hexdigest(),
                    "window_start": window_start,
                    "window_end": window_end,
                    "user_id": user_id,
                    "event_count": sum(values.values()),
                    "pv_count": values["pv"],
                    "cart_count": values["cart"],
                    "fav_count": values["fav"],
                    "buy_count": values["buy"],
                    "distinct_items": len(feature_items[key]),
                    "feature_version": window_end,
                }
            )

        quality = []
        for event in invalid:
            quality.append(self.quality_row(event, "INVALID", "INVALID_IDENTIFIER"))
        for event in late:
            quality.append(self.quality_row(event, "LATE", "LATE_FOR_AGGREGATION"))
        quality.sort(key=lambda row: expected["quality"].index(row))

        carts: dict[str, dict[str, str]] = {"1": {}, "2": {}}
        cart_state: dict[tuple[int, int], tuple[int, int, int]] = {}
        ordering: dict[tuple[int, int], tuple[int, int]] = {}
        for event in accepted:
            if event.behavior_type not in {"cart", "buy"}:
                continue
            key = (event.user_id, event.item_id)
            order = (event.event_time_ms, event.source_sequence)
            if order <= ordering.get(key, (-1, -1)):
                continue
            ordering[key] = order
            if event.behavior_type == "buy":
                carts[str(event.user_id)].pop(str(event.item_id), None)
                continue
            added_at = event.event_time_ms
            cart_state[key] = (event.category_id, added_at, event.event_time_ms)
            carts[str(event.user_id)][str(event.item_id)] = (
                f"{event.category_id}|{added_at}|{event.event_time_ms}"
            )

        accounting = {
            "replay_attempts": 2,
            "source_rows": len(attempts),
            "decoded": len(attempts),
            "invalid": len(invalid),
            "valid": len(valid),
            "duplicate": len(duplicates),
            "accepted_unique": len(accepted),
            "on_time": len(on_time),
            "late_for_aggregation": len(late),
            "canonical_raw": len(raw),
            "metrics_input": len(on_time),
            "producer_rejected": 0,
        }
        self.assertEqual(expected["accounting"], accounting)
        self.assertEqual(expected["canonical_raw"], raw)
        self.assertEqual(expected["metrics"], metrics)
        self.assertEqual(expected["user_features"], user_features)
        self.assertEqual(expected["quality"], quality)
        self.assertEqual(expected["redis_active_carts"], carts)

    @staticmethod
    def quality_row(event, quality_type: str, reason_code: str) -> dict[str, object]:
        return {
            "quality_event_id": hashlib.sha256(
                "|".join(
                    (
                        quality_type,
                        event.event_id,
                        event.replay_run_id,
                        str(event.source_sequence),
                        reason_code,
                    )
                ).encode()
            ).hexdigest(),
            "quality_type": quality_type,
            "event_id": event.event_id,
            "replay_run_id": event.replay_run_id,
            "source_sequence": event.source_sequence,
            "reason_code": reason_code,
        }


if __name__ == "__main__":
    unittest.main()
