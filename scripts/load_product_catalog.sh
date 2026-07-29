#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

catalog_csv="${1:-artifacts/product_catalog.csv}"
manifest_json="${2:-artifacts/product_catalog_manifest.json}"
compose=(docker compose -f infra/docker-compose.yml --profile catalog)

if [[ ! -f "$catalog_csv" || ! -f "$manifest_json" ]]; then
  echo "Catalog CSV or manifest is missing. Generate both before loading." >&2
  exit 1
fi

expected_products="$(
  python -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["unique_products"])' \
    "$manifest_json"
)"

{
  cat <<'SQL'
\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE product_catalog_stage (LIKE product_catalog INCLUDING ALL);
\copy product_catalog_stage(product_id,category_id,product_name,price,is_active,updated_at,catalog_version) FROM STDIN WITH (FORMAT csv, HEADER true)
SQL
  cat "$catalog_csv"
  cat <<'SQL'
\.
TRUNCATE TABLE product_catalog;
INSERT INTO product_catalog SELECT * FROM product_catalog_stage;
COMMIT;
SELECT COUNT(*) AS loaded_products FROM product_catalog;
SQL
} | "${compose[@]}" exec -T postgres psql \
  --username "${POSTGRES_USER:-catalog}" \
  --dbname "${POSTGRES_DB:-catalog}"

actual_products="$(
  "${compose[@]}" exec -T postgres psql \
    --username "${POSTGRES_USER:-catalog}" \
    --dbname "${POSTGRES_DB:-catalog}" \
    --tuples-only --no-align \
    --command "SELECT COUNT(*) FROM product_catalog"
)"

if [[ "$actual_products" != "$expected_products" ]]; then
  echo "Catalog reconciliation failed: expected=$expected_products actual=$actual_products" >&2
  exit 1
fi
echo "Catalog load reconciled: $actual_products products."
