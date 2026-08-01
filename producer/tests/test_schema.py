from __future__ import annotations

import json
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCHEMA = REPOSITORY_ROOT / "schemas/user-behavior-event.avsc"


class SchemaTests(unittest.TestCase):
    def test_schema_matches_phase_one_contract(self) -> None:
        schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
        self.assertEqual(schema["name"], "UserBehaviorEvent")
        self.assertEqual(
            [field["name"] for field in schema["fields"]],
            [
                "event_id",
                "user_id",
                "item_id",
                "category_id",
                "behavior_type",
                "event_time_ms",
                "source_sequence",
                "replay_run_id",
            ],
        )
        self.assertEqual(schema["fields"][4]["type"]["symbols"], ["pv", "cart", "fav", "buy"])


if __name__ == "__main__":
    unittest.main()
