from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts/canonical_snapshot.py"


def load_module():
    spec = importlib.util.spec_from_file_location("canonical_snapshot", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CanonicalSnapshotTests(unittest.TestCase):
    def test_comparison_uses_only_stable_business_results(self) -> None:
        snapshot = {
            "canonical_raw": [{"event_id": "a", "item_id": 1}],
            "canonical_metrics": [{"window_start": 0, "item_id": 1, "pv_count": 1}],
        }
        load_module().compare_snapshots(snapshot, dict(snapshot))

    def test_comparison_fails_with_a_useful_section_name(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(ValueError, "canonical_metrics differs"):
            module.compare_snapshots(
                {"canonical_raw": [], "canonical_metrics": [{"pv_count": 1}]},
                {"canonical_raw": [], "canonical_metrics": [{"pv_count": 2}]},
            )


if __name__ == "__main__":
    unittest.main()
