CREATE SCHEMA orders;


CREATE TABLE orders.raw_orders_shopify (
    -- Primary Key
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    -- CORE ORDER FIELDS
    name VARCHAR(100),                  -- Order number (e.g., #1001)
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    cancel_reason TEXT,
    processed_at TIMESTAMP,
    closed_at TIMESTAMP,
    confirmed BOOLEAN,
    test BOOLEAN,
    tags TEXT,               -- Comma-separated or JSON array
    note TEXT,
    source_name VARCHAR(100),
    
    -- FINANCIAL TOTALS (all in shop's base currency)
    current_subtotal_price DECIMAL(15, 2),
    current_total_tax DECIMAL(15, 2),
    current_total_discounts DECIMAL(15, 2),
    current_total_price DECIMAL(15, 2),
    total_shipping_price DECIMAL(15, 2),
    total_refunded DECIMAL(15, 2),
    currency_code VARCHAR(3),             -- ISO 4217 (e.g., USD, EUR, AUD)
    
    -- BILLING ADDRESS
    billing_first_name VARCHAR(100),
    billing_last_name VARCHAR(100),
    billing_company VARCHAR(255),
    billing_address1 VARCHAR(255),
    billing_address2 VARCHAR(255),
    billing_city VARCHAR(100),
    billing_province VARCHAR(100),
    billing_province_code VARCHAR(10),
    billing_zip VARCHAR(20),
    billing_country VARCHAR(100),
    billing_country_code VARCHAR(2),      -- ISO 3166-1 alpha-2
    billing_phone VARCHAR(50),
    
    -- SHIPPING ADDRESS
    shipping_first_name VARCHAR(100),
    shipping_last_name VARCHAR(100),
    shipping_company VARCHAR(255),
    shipping_address1 VARCHAR(255),
    shipping_address2 VARCHAR(255),
    shipping_city VARCHAR(100),
    shipping_province VARCHAR(100),
    shipping_province_code VARCHAR(10),
    shipping_zip VARCHAR(20),
    shipping_country VARCHAR(100),
    shipping_country_code VARCHAR(2),     -- ISO 3166-1 alpha-2
    shipping_phone VARCHAR(50),
    
    -- FULFILLMENT STATUS (aggregated/latest)
    fulfillment_status VARCHAR(50),       -- Latest fulfillment status
    fulfillment_display_status VARCHAR(50) -- Human-readable status
);

ALTER TABLE orders.raw_orders_shopify
ADD CONSTRAINT fk_customer
  FOREIGN KEY (customer_id)
  REFERENCES customers.raw_customers_shopify(customer_id)
  ON DELETE SET NULL;

ALTER TABLE orders.raw_orders_shopify
DROP CONSTRAINT fk_customer;


CREATE TRIGGER trg_set_timestamp
BEFORE UPDATE ON orders.raw_orders_shopify
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(updated_at);



CREATE TABLE orders.raw_parcels_shopify (

    -- =========================
    -- IDENTIFIERS
    -- =========================
    parcel_id BIGINT PRIMARY KEY,                     -- core: fulfillment.id or unique per parcel
    fulfillment_id BIGINT,                            -- link to fulfillment
    order_id BIGINT NOT NULL,                         -- parent order
    fulfillment_order_id BIGINT,                      -- optional: fulfillment order link

        -- =========================
    -- AUDIT FIELDS
    -- =========================
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- =========================
    -- TRACKING INFORMATION
    -- =========================
    tracking_company VARCHAR(255),
    tracking_number VARCHAR(255),
    tracking_url TEXT,
    tracking_status VARCHAR(100),                     -- e.g. IN_TRANSIT, DELIVERED

    -- =========================
    -- SHIPPING INFORMATION
    -- =========================
    shipped_at TIMESTAMPTZ,                           -- shipped date/time
    delivered_at TIMESTAMPTZ,                         -- delivered timestamp
    estimated_delivery_at TIMESTAMPTZ,

    carrier_identifier VARCHAR(255),                  -- carrierService.id
    carrier_service_name VARCHAR(255),                -- carrierService.name
    shipping_method VARCHAR(255),                     -- delivery method
    service_code VARCHAR(100),                        -- internal code

    -- =========================
    -- PARCEL WEIGHT / SIZE
    -- =========================
    weight_value NUMERIC(12,4),                       -- actual weight
    weight_unit VARCHAR(20),                          -- kg, g, lb

    dimension_unit VARCHAR(20),                       -- cm, in
    dimension_height NUMERIC(12,4),
    dimension_width NUMERIC(12,4),
    dimension_length NUMERIC(12,4),

    -- =========================
    -- ADDRESS ORIGIN / DESTINATION
    -- =========================
    origin_name VARCHAR(255),
    origin_company VARCHAR(255),
    origin_address1 VARCHAR(255),
    origin_address2 VARCHAR(255),
    origin_city VARCHAR(255),
    origin_province VARCHAR(255),
    origin_province_code VARCHAR(50),
    origin_zip VARCHAR(20),
    origin_country VARCHAR(255),
    origin_country_code VARCHAR(10),
    origin_phone VARCHAR(50),

    destination_name VARCHAR(255),
    destination_company VARCHAR(255),
    destination_address1 VARCHAR(255),
    destination_address2 VARCHAR(255),
    destination_city VARCHAR(255),
    destination_province VARCHAR(255),
    destination_province_code VARCHAR(50),
    destination_zip VARCHAR(20),
    destination_country VARCHAR(255),
    destination_country_code VARCHAR(10),
    destination_phone VARCHAR(50),

    -- =========================
    -- MISC / METADATA
    -- =========================
    status VARCHAR(100),                              -- fulfillment_status
    package_type VARCHAR(100),                        -- Shopify package preset
    is_final BOOLEAN,                                 -- final package for fulfillment

    FOREIGN KEY (order_id) REFERENCES orders.raw_orders_shopify(order_id)
);


CREATE TRIGGER trg_set_timestamp
BEFORE UPDATE ON orders.raw_parcels_shopify
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(updated_at);