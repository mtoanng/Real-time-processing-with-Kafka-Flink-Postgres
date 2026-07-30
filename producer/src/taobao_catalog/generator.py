"""Generate deterministic synthetic product metadata from valid Taobao items.

The catalog is an optional operational fixture, not an attribute set supplied
by the Alibaba/Taobao source dataset.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path

from taobao_replay.contracts import BEHAVIOR_TYPES
from taobao_replay.reader import iter_source_rows

CATALOG_UPDATED_AT = "2026-01-01T00:00:00Z"


@dataclass(frozen=True, slots=True)
class CatalogManifest:
    """Coverage and reproducibility evidence for a generated catalog fixture."""

    source_rows: int
    valid_source_rows: int
    rejected_source_rows: int
    unique_products: int
    catalog_sha256: str

    def to_dict(self) -> dict[str, int | str]:
        """Return JSON-safe manifest fields."""
        return asdict(self)


def _parse_catalog_key(values: tuple[str, ...]) -> tuple[int, int] | None:
    if len(values) != 5:
        return None
    user_raw, item_raw, category_raw, behavior_raw, timestamp_raw = (
        value.strip() for value in values
    )
    try:
        user_id = int(user_raw)
        item_id = int(item_raw)
        category_id = int(category_raw)
        timestamp = int(timestamp_raw)
    except ValueError:
        return None
    if (
        user_id <= 0
        or item_id <= 0
        or category_id <= 0
        or timestamp <= 0
        or behavior_raw not in BEHAVIOR_TYPES
    ):
        return None
    return item_id, category_id


def _price(product_id: int) -> str:
    return str(
        (Decimal(product_id % 50_000) / Decimal(100) + Decimal("1.00")).quantize(Decimal("0.01"))
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generate_catalog(source: Path, output: Path, manifest_path: Path) -> CatalogManifest:
    """Create one deterministic synthetic catalog row for every valid source item.

    SQLite enforces that a source item cannot silently map to conflicting source
    categories while keeping memory bounded for a full input file.
    """
    if not source.is_file():
        raise FileNotFoundError(f"source dataset not found: {source}")

    output.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    source_rows = 0
    valid_source_rows = 0
    rejected_source_rows = 0

    with tempfile.TemporaryDirectory(prefix="taobao-catalog-") as temp_directory:
        database_path = Path(temp_directory) / "catalog.sqlite3"
        connection = sqlite3.connect(database_path)
        try:
            connection.execute(
                "CREATE TABLE products ("
                "product_id INTEGER PRIMARY KEY, category_id INTEGER NOT NULL)"
            )
            connection.execute(
                "CREATE TEMP TABLE incoming_products ("
                "product_id INTEGER NOT NULL, category_id INTEGER NOT NULL)"
            )
            pending: list[tuple[int, int]] = []

            def flush_pending() -> None:
                if not pending:
                    return
                connection.execute("DELETE FROM incoming_products")
                connection.executemany(
                    "INSERT INTO incoming_products(product_id, category_id) VALUES (?, ?)",
                    pending,
                )
                conflict = connection.execute(
                    "SELECT product_id, MIN(category_id), MAX(category_id) "
                    "FROM incoming_products GROUP BY product_id "
                    "HAVING MIN(category_id) <> MAX(category_id) LIMIT 1"
                ).fetchone()
                if conflict is None:
                    conflict = connection.execute(
                        "SELECT i.product_id, p.category_id, i.category_id "
                        "FROM incoming_products i JOIN products p USING(product_id) "
                        "WHERE i.category_id <> p.category_id LIMIT 1"
                    ).fetchone()
                if conflict is not None:
                    raise ValueError(
                        "source item maps to multiple categories: "
                        f"item_id={conflict[0]}, categories={conflict[1]},{conflict[2]}"
                    )
                connection.execute(
                    "INSERT OR IGNORE INTO products(product_id, category_id) "
                    "SELECT DISTINCT product_id, category_id FROM incoming_products"
                )
                connection.commit()
                pending.clear()

            for _, values in iter_source_rows(source):
                source_rows += 1
                key = _parse_catalog_key(values)
                if key is None:
                    rejected_source_rows += 1
                    continue
                valid_source_rows += 1
                pending.append(key)
                if len(pending) == 10_000:
                    flush_pending()
            flush_pending()

            temporary_output = output.with_suffix(f"{output.suffix}.tmp")
            with temporary_output.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(
                    (
                        "product_id",
                        "category_id",
                        "product_name",
                        "price",
                        "is_active",
                        "updated_at",
                        "catalog_version",
                    )
                )
                for product_id, category_id in connection.execute(
                    "SELECT product_id, category_id FROM products ORDER BY product_id"
                ):
                    writer.writerow(
                        (
                            product_id,
                            category_id,
                            f"Synthetic product {product_id}",
                            _price(product_id),
                            "true",
                            CATALOG_UPDATED_AT,
                            1,
                        )
                    )
            temporary_output.replace(output)
            unique_products = connection.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        finally:
            connection.close()

    manifest = CatalogManifest(
        source_rows=source_rows,
        valid_source_rows=valid_source_rows,
        rejected_source_rows=rejected_source_rows,
        unique_products=unique_products,
        catalog_sha256=_sha256(output),
    )
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a bounded-memory synthetic product catalog for all valid Taobao items."
        )
    )
    parser.add_argument("source", type=Path, help="UserBehavior.csv path")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/product_catalog.csv"),
        help="generated catalog CSV (default: artifacts/product_catalog.csv)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("artifacts/product_catalog_manifest.json"),
        help="coverage manifest (default: artifacts/product_catalog_manifest.json)",
    )
    args = parser.parse_args(argv)
    manifest = generate_catalog(args.source, args.output, args.manifest)
    print(json.dumps(manifest.to_dict(), sort_keys=True))
    return 0
