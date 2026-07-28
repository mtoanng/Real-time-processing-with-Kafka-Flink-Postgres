CREATE TABLE product_catalog (
    product_id BIGINT PRIMARY KEY CHECK (product_id > 0),
    category_id BIGINT NOT NULL CHECK (category_id > 0),
    product_name TEXT NOT NULL CHECK (length(trim(product_name)) > 0),
    price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL,
    catalog_version BIGINT NOT NULL CHECK (catalog_version > 0)
);

CREATE FUNCTION enforce_product_catalog_version()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.catalog_version < OLD.catalog_version THEN
        RAISE EXCEPTION 'catalog_version cannot move backwards';
    END IF;
    IF NEW.catalog_version = OLD.catalog_version
       AND ROW(
           NEW.category_id,
           NEW.product_name,
           NEW.price,
           NEW.is_active,
           NEW.updated_at
       ) IS DISTINCT FROM ROW(
           OLD.category_id,
           OLD.product_name,
           OLD.price,
           OLD.is_active,
           OLD.updated_at
       ) THEN
        RAISE EXCEPTION 'changed catalog content requires a newer catalog_version';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER product_catalog_version_guard
BEFORE UPDATE ON product_catalog
FOR EACH ROW
EXECUTE FUNCTION enforce_product_catalog_version();

CREATE FUNCTION reject_product_catalog_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'deactivate products with is_active=false';
END;
$$;

CREATE TRIGGER product_catalog_no_delete
BEFORE DELETE ON product_catalog
FOR EACH ROW
EXECUTE FUNCTION reject_product_catalog_delete();

INSERT INTO product_catalog (
    product_id,
    category_id,
    product_name,
    price,
    is_active,
    updated_at,
    catalog_version
) VALUES
    (100, 10, 'Wireless Headphones', 49.99, TRUE, '2026-07-29T01:00:00Z', 1),
    (101, 11, 'Wireless Mouse', 19.99, TRUE, '2026-07-29T01:00:00Z', 1),
    (102, 12, 'Mechanical Keyboard', 79.00, TRUE, '2026-07-29T01:00:00Z', 1),
    (103, 13, 'USB-C Charger', 29.50, TRUE, '2026-07-29T01:00:00Z', 1),
    (104, 14, 'Action Camera', 129.00, TRUE, '2026-07-29T01:00:00Z', 1);
