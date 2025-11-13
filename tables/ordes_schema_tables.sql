CREATE SCHEMA orders;

CREATE TABLE orders.raw_orders_shopify (
    -- ===== ID =====
    order_id BIGINT PRIMARY KEY,
    order_name VARCHAR(100),

    -- ===== CUSTOMER =====
    customer_id BIGINT,

    -- ===== AUDIT =====
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    notes TEXT,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    vendor VARCHAR(255),
    tags TEXT,
    order_source VARCHAR(100),

    -- ===== SUPPLY =====
    fulfillment_status VARCHAR(100),
    fulfilled_at TIMESTAMP WITH TIME ZONE,

    -- ===== PAYMENTS =====
    payment_status VARCHAR(100),
    paid_at TIMESTAMP WITH TIME ZONE,
    currency VARCHAR(10),
    subtotal_pre_tax NUMERIC(12,2),
    taxes NUMERIC(12,2),
    shipping_fees NUMERIC(12,2),
    order_amount NUMERIC(12,2),
    discount_code VARCHAR(100),
    discount_amount NUMERIC(12,2),
    shipping_method VARCHAR(100),
    payment_method VARCHAR(100),
    payment_id VARCHAR(100),
    refound_amount NUMERIC(12,2),
    risk_level VARCHAR(50),
    receipt_number VARCHAR(100),

    -- ===== BILLING =====
    billing_name VARCHAR(255),
    billing_address VARCHAR(255),
    billing_address_1 VARCHAR(255),
    billing_address_2 VARCHAR(255),
    billing_company VARCHAR(255),
    billing_city VARCHAR(100),
    billing_zip_code VARCHAR(20),
    billing_state VARCHAR(100),
    billing_country VARCHAR(100),
    billing_phone VARCHAR(50),

    -- ===== SHIPPING =====
    shipping_name VARCHAR(255),
    shipping_street VARCHAR(255),
    shipping_address_1 VARCHAR(255),
    shipping_address_2 VARCHAR(255),
    shipping_company VARCHAR(255),
    shipping_city VARCHAR(100),
    shipping_zip_code VARCHAR(20),
    shipping_state VARCHAR(100),
    shipping_country VARCHAR(100),
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
    -- ===== ID =====
    parcel_id BIGINT PRIMARY KEY,
    product_id BIGINT,
    order_id BIGINT REFERENCES orders.raw_orders_shopify(order_id),

    -- ===== PRODUCT =====
    product_name VARCHAR(255),

    -- ===== AUDIT =====
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    canceled_at TIMESTAMP WITH TIME ZONE,
    notes TEXT,

    -- ===== QUANTITY =====
    quantity INTEGER DEFAULT 0,

    -- ===== PRICE =====
    product_unit_price NUMERIC(12,2),
    product_unit_discount NUMERIC(12,2),

    -- ===== TAX =====
    tax_flag BOOLEAN DEFAULT FALSE,

    -- ===== SUPPLY =====
    fulfillment_status VARCHAR(100),

    -- ===== SHIPPING =====
    shipping_flag BOOLEAN DEFAULT FALSE
);

CREATE TRIGGER trg_set_timestamp
BEFORE UPDATE ON orders.raw_parcels_shopify
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(updated_at);