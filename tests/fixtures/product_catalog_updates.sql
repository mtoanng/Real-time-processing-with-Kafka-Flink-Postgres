UPDATE product_catalog
SET product_name = 'Wireless Headphones Pro',
    price = 44.99,
    updated_at = '2026-07-29T01:05:00Z',
    catalog_version = 2
WHERE product_id = 100;

UPDATE product_catalog
SET is_active = FALSE,
    updated_at = '2026-07-29T01:06:00Z',
    catalog_version = 2
WHERE product_id = 101;

-- Intentional same-version retry: identical content is allowed and downstream
-- replacement remains deterministic.
UPDATE product_catalog
SET product_name = 'Wireless Headphones Pro',
    price = 44.99,
    updated_at = '2026-07-29T01:05:00Z',
    catalog_version = 2
WHERE product_id = 100;
