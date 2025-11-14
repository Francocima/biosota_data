CREATE SCHEMA orders;


DROP CONSTRAINT

CREATE TABLE orders.raw_orders_shopify (
    -- =========================
    -- ORDER IDENTIFICATION
    -- =========================
    order_id BIGINT PRIMARY KEY,                  -- Shopify numeric ID
    order_name VARCHAR(100),                      -- e.g. "#1001"
    order_number INT,                             -- sequential number
    email VARCHAR(255),
    phone VARCHAR(50),
    customer_id BIGINT,                           -- matches Shopify Customer ID

    -- =========================
    -- AUDIT FIELDS
    -- =========================
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    -- =========================
    -- ORDER STATUS
    -- =========================
    financial_status VARCHAR(50),                 -- e.g. PAID, PARTIALLY_REFUNDED
    fulfillment_status VARCHAR(50),               -- e.g. FULFILLED, PARTIAL
    cancelled_at TIMESTAMPTZ,
    cancel_reason VARCHAR(100),
    processed_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    confirmed BOOLEAN,
    test BOOLEAN,                                 -- Shopify test order flag

    -- =========================
    -- TAGS, NOTES & ORIGINS
    -- =========================
    tags TEXT,
    note TEXT,
    referring_site TEXT,
    landing_site TEXT,
    source_name VARCHAR(100),                     -- web, pos, shopify draft, etc.
    referring_site_url TEXT,

    -- =========================
    -- PRICE / MONEY FIELDS
    -- All from Shopify Order object
    -- =========================
    currency VARCHAR(10),
    
    subtotal_price NUMERIC(12,2),
    total_tax NUMERIC(12,2),
    total_tip NUMERIC(12,2),
    total_discounts NUMERIC(12,2),
    total_shipping_price NUMERIC(12,2),
    total_price NUMERIC(12,2),

    refund_amount NUMERIC(12,2),                  -- sum(refunds[].transactions.amount)

    -- =========================
    -- DISCOUNT + SHIPPING DETAILS
    -- =========================
    discount_code VARCHAR(100),
    discount_code_type VARCHAR(50),
    discount_code_amount NUMERIC(12,2),

    shipping_method VARCHAR(255),
    shipping_carrier VARCHAR(255),

    -- =========================
    -- PAYMENT DETAILS
    -- =========================
    payment_gateway VARCHAR(100),                 -- e.g. "shopify_payments"
    payment_method VARCHAR(100),
    payment_id VARCHAR(200),                      -- transaction ID
    risk_level VARCHAR(50),
    receipt_number VARCHAR(100),

    -- =========================
    -- BILLING ADDRESS
    -- matches Shopify MailingAddress
    -- =========================
    billing_first_name VARCHAR(255),
    billing_last_name VARCHAR(255),
    billing_company VARCHAR(255),
    billing_address1 VARCHAR(255),
    billing_address2 VARCHAR(255),
    billing_city VARCHAR(255),
    billing_province VARCHAR(255),
    billing_province_code VARCHAR(50),
    billing_zip VARCHAR(20),
    billing_country VARCHAR(255),
    billing_country_code VARCHAR(10),
    billing_phone VARCHAR(50),

    -- =========================
    -- SHIPPING ADDRESS
    -- matches Shopify MailingAddress
    -- =========================
    shipping_first_name VARCHAR(255),
    shipping_last_name VARCHAR(255),
    shipping_company VARCHAR(255),
    shipping_address1 VARCHAR(255),
    shipping_address2 VARCHAR(255),
    shipping_city VARCHAR(255),
    shipping_province VARCHAR(255),
    shipping_province_code VARCHAR(50),
    shipping_zip VARCHAR(20),
    shipping_country VARCHAR(255),
    shipping_country_code VARCHAR(10),
    shipping_phone VARCHAR(50)
);

ALTER TABLE orders.raw_orders_shopify
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_id)
REFERENCES customers.raw_customers_shopify(customer_id);

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