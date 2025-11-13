CREATE TABLE products.raw_products_shopify (
    product_id      TEXT PRIMARY KEY,
    product_name    TEXT NOT NULL,
    product_desc    TEXT,
    product_price   NUMERIC(10,2) NOT NULL CHECK (product_price >= 0),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ
);

CREATE EXTENSION IF NOT EXISTS moddatetime;

CREATE TRIGGER trg_set_timestamp
BEFORE UPDATE ON products.raw_products_shopify
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(updated_at);