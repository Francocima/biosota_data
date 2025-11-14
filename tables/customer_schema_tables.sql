CREATE TABLE customers.raw_customers_shopify (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    accepts_email_marketing BOOLEAN DEFAULT FALSE,
    address_company VARCHAR(255),
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    city VARCHAR(100),
    state_code VARCHAR(10),
    country_code VARCHAR(10),
    zip_code VARCHAR(20),
    phone VARCHAR(50),
    accepts_sms_marketing BOOLEAN DEFAULT FALSE,
    note TEXT,
    tax_exempt BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

ALTER TABLE customers.raw_customers_shopify
  ADD COLUMN verified_email               BOOLEAN,
  ADD COLUMN valid_email_address          BOOLEAN,
  ADD COLUMN email_marketing_state        VARCHAR(50),
  ADD COLUMN email_marketing_opt_in_level VARCHAR(50),
  ADD COLUMN email_consent_updated_at     TIMESTAMP,
  ADD COLUMN sms_marketing_state          VARCHAR(50),
  ADD COLUMN sms_marketing_opt_in_level   VARCHAR(50),
  ADD COLUMN sms_consent_updated_at       TIMESTAMP,
  ADD COLUMN display_name                 VARCHAR(200);


ALTER TABLE customers.raw_customers_shopify
    drop column accepts_email_marketing,
    drop column accepts_sms_marketing;

CREATE TRIGGER trg_set_timestamp
BEFORE UPDATE ON customers.raw_customers_shopify
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(updated_at);


