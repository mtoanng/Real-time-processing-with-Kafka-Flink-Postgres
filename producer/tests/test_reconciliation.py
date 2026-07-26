from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts/reconcile_final_e2e.py"
FIXTURE = REPOSITORY_ROOT / "tests/fixtures/user_behavior_fixture.csv"


def load_reconciliation_module():
    spec = importlib.util.spec_from_file_location("reconcile_final_e2e", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReconciliationTests(unittest.TestCase):
    def test_single_fixture_run_reconciles_the_core_equations(self) -> None:
        result = load_reconciliation_module().build_expected(FIXTURE, ["run-a"])

        self.assertEqual(1_000, result["fixture_source_rows"])
        self.assertEqual(1_000, result["flink_decoded_rows"])
        self.assertEqual(1, result["invalid_events"])
        self.assertEqual(0, result["duplicate_events"])
        self.assertEqual(999, result["accepted_unique_events"])
        self.assertEqual(2, result["late_for_aggregation_events"])
        self.assertEqual(997, result["on_time_events"])
        self.assertEqual(999, result["canonical_raw_events"])
        self.assertEqual([501], result["user_100_active_cart_item_ids"])

    def test_two_replay_runs_have_same_canonical_result_and_count_duplicates(self) -> None:
        module = load_reconciliation_module()
        run_a = module.build_expected(FIXTURE, ["run-a"])
        run_a_and_b = module.build_expected(FIXTURE, ["run-a", "run-b"])

        self.assertEqual(2_000, run_a_and_b["flink_decoded_rows"])
        self.assertEqual(2, run_a_and_b["invalid_events"])
        self.assertEqual(999, run_a_and_b["duplicate_events"])
        self.assertEqual(999, run_a_and_b["accepted_unique_events"])
        self.assertEqual(run_a["canonical_raw_digest"], run_a_and_b["canonical_raw_digest"])
        self.assertEqual(run_a["canonical_metric_digest"], run_a_and_b["canonical_metric_digest"])
        self.assertEqual(run_a["item_500_metric_values"], run_a_and_b["item_500_metric_values"])

    def test_committed_expected_evidence_matches_independent_computation(self) -> None:
        actual = load_reconciliation_module().build_expected(
            FIXTURE, ["fixture-run-a", "fixture-run-b"]
        )
        actual["fixture"] = "tests/fixtures/user_behavior_fixture.csv"
        expected = json.loads(
            (REPOSITORY_ROOT / "docs/evidence/latest/expected-reconciliation.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()
