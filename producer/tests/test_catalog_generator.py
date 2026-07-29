from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from taobao_catalog.generator import generate_catalog

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/user_behavior_fixture.csv"


class CatalogGeneratorTests(unittest.TestCase):
    def test_fixture_catalog_covers_every_semantically_valid_item(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "catalog.csv"
            manifest_path = Path(directory) / "manifest.json"
            manifest = generate_catalog(FIXTURE, output, manifest_path)
            with output.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(
            [
                (str(item), str(category))
                for item, category in zip(range(100, 105), range(10, 15), strict=True)
            ],
            [(row["product_id"], row["category_id"]) for row in rows],
        )
        self.assertNotIn("200", {row["product_id"] for row in rows})
        self.assertEqual(12, manifest.source_rows)
        self.assertEqual(11, manifest.valid_source_rows)
        self.assertEqual(1, manifest.rejected_source_rows)
        self.assertEqual(5, manifest.unique_products)

    def test_generation_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            first = generate_catalog(FIXTURE, base / "first.csv", base / "first.json")
            second = generate_catalog(FIXTURE, base / "second.csv", base / "second.json")

            self.assertEqual((base / "first.csv").read_bytes(), (base / "second.csv").read_bytes())
            self.assertEqual(first, second)
            self.assertEqual(first.to_dict(), json.loads((base / "first.json").read_text()))

    def test_conflicting_source_categories_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source = base / "conflict.csv"
            source.write_text(
                "1,10,20,pv,1511658000\n2,10,21,cart,1511658001\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "item_id=10, categories=20,21"):
                generate_catalog(source, base / "catalog.csv", base / "manifest.json")


if __name__ == "__main__":
    unittest.main()
