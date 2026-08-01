from __future__ import annotations

import unittest

from taobao_replay.contracts import RowValidationError, parse_event


class ContractTests(unittest.TestCase):
    def test_event_id_is_stable_across_replay_runs(self) -> None:
        row = ("1", "2", "3", "pv", "1511658000")
        first = parse_event(row, source_sequence=7, replay_run_id="run-a")
        second = parse_event(row, source_sequence=7, replay_run_id="run-b")
        self.assertEqual(first.event_id, second.event_id)
        self.assertNotEqual(first.replay_run_id, second.replay_run_id)
        self.assertEqual(first.event_time_ms, 1_511_658_000_000)

    def test_event_id_changes_with_source_sequence(self) -> None:
        row = ("1", "2", "3", "pv", "1511658000")
        first = parse_event(row, source_sequence=7, replay_run_id="run")
        second = parse_event(row, source_sequence=8, replay_run_id="run")
        self.assertNotEqual(first.event_id, second.event_id)

    def test_event_id_changes_with_stable_source_field(self) -> None:
        first = parse_event(("1", "2", "3", "pv", "10"), source_sequence=7, replay_run_id="run")
        second = parse_event(("1", "9", "3", "pv", "10"), source_sequence=7, replay_run_id="run")
        self.assertNotEqual(first.event_id, second.event_id)

    def test_invalid_values_are_rejected(self) -> None:
        invalid_rows = [
            ("1", "2", "3", "unknown", "1511658000"),
            ("zero", "2", "3", "pv", "1511658000"),
            ("1", "2", "3", "pv"),
        ]
        for row in invalid_rows:
            with self.subTest(row=row), self.assertRaises(RowValidationError):
                parse_event(row, source_sequence=0, replay_run_id="test")

    def test_semantic_invalid_values_remain_encodable_for_flink_validation(self) -> None:
        event = parse_event(("0", "2", "3", "pv", "-1"), source_sequence=0, replay_run_id="test")

        self.assertEqual(0, event.user_id)
        self.assertEqual(-1_000, event.event_time_ms)


if __name__ == "__main__":
    unittest.main()
