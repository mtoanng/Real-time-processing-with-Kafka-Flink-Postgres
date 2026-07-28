from __future__ import annotations

import json
import unittest
from collections import Counter, defaultdict
from hashlib import sha256
from pathlib import Path

from taobao_replay.reader import iter_event_batches

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/user_behavior_fixture.csv"
EXPECTED = ROOT / "tests/fixtures/golden_outputs.json"
RUN_IDS = ("golden-a", "golden-b")
MAX_OUT_OF_ORDERNESS_MS = 5_000


def quality_id(
    quality_type: str,
    event_id: str,
    replay_run_id: str,
    source_sequence: int,
    reason_code: str,
) -> str:
    value = "\x1f".join((quality_type, event_id, replay_run_id, str(source_sequence), reason_code))
    return sha256(value.encode()).hexdigest()


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
            watermark = maximum - MAX_OUT_OF_ORDERNESS_MS - 1
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
        for event in on_time:
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

        quality = []
        for event in invalid:
            quality.append(self.quality_row(event, "INVALID", "INVALID_IDENTIFIER"))
        for event in late:
            quality.append(self.quality_row(event, "LATE", "LATE_FOR_AGGREGATION"))
        for event in duplicates:
            quality.append(self.quality_row(event, "DUPLICATE", "DUPLICATE_WITHIN_RETENTION"))
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
            added_at = cart_state.get(key, (event.category_id, event.event_time_ms, 0))[1]
            cart_state[key] = (event.category_id, added_at, event.event_time_ms)
            carts[str(event.user_id)][str(event.item_id)] = (
                f"{event.category_id}|{added_at}|{event.event_time_ms}"
            )

        accounting = {
            "replay_attempts": 2,
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
        self.assertEqual(expected["quality"], quality)
        self.assertEqual(expected["redis_active_carts"], carts)

    @staticmethod
    def quality_row(event, quality_type: str, reason_code: str) -> dict[str, object]:
        return {
            "quality_event_id": quality_id(
                quality_type,
                event.event_id,
                event.replay_run_id,
                event.source_sequence,
                reason_code,
            ),
            "quality_type": quality_type,
            "event_id": event.event_id,
            "replay_run_id": event.replay_run_id,
            "source_sequence": event.source_sequence,
            "reason_code": reason_code,
        }


if __name__ == "__main__":
    unittest.main()
