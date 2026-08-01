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
        self.assertEqual(0, manifest.category_conflict_products)
        self.assertEqual(
            "most_frequent_then_lowest_category_id",
            manifest.category_resolution_policy,
        )

    def test_generation_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            first = generate_catalog(FIXTURE, base / "first.csv", base / "first.json")
            second = generate_catalog(FIXTURE, base / "second.csv", base / "second.json")

            self.assertEqual((base / "first.csv").read_bytes(), (base / "second.csv").read_bytes())
            self.assertEqual(first, second)
            self.assertEqual(first.to_dict(), json.loads((base / "first.json").read_text()))

    def test_conflicting_source_categories_use_deterministic_dominant_category(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source = base / "conflict.csv"
            source.write_text(
                "1,10,21,pv,1511658000\n"
                "2,10,20,cart,1511658001\n"
                "3,10,21,buy,1511658002\n"
                "4,11,30,pv,1511658003\n"
                "5,11,29,pv,1511658004\n",
                encoding="utf-8",
            )
            output = base / "catalog.csv"
            manifest = generate_catalog(source, output, base / "manifest.json")
            with output.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(
                [("10", "21"), ("11", "29")],
                [(row["product_id"], row["category_id"]) for row in rows],
            )
            self.assertEqual(2, manifest.category_conflict_products)


if __name__ == "__main__":
    unittest.main()
