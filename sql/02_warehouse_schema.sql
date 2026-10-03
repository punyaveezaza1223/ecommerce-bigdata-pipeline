-- ============================================================
-- E-COMMERCE ANALYTICS DATA WAREHOUSE
-- Star Schema
-- ============================================================


-- ============================================================
-- 1. DIMENSION: DATE
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN DEFAULT FALSE
);


-- ============================================================
-- 2. DIMENSION: PLATFORM
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_platform (
    platform_key SERIAL PRIMARY KEY,
    platform_name VARCHAR(50) NOT NULL UNIQUE,
    platform_type VARCHAR(20) NOT NULL
);


-- ============================================================
-- 3. DIMENSION: PRODUCT
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGSERIAL PRIMARY KEY,

    source_system VARCHAR(50) NOT NULL,
    source_product_id VARCHAR(255) NOT NULL,

    product_name TEXT NOT NULL,
    category TEXT,
    brand TEXT,

    sku VARCHAR(255),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (source_system, source_product_id)
);


-- ============================================================
-- 4. DIMENSION: CUSTOMER
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,

    source_customer_id BIGINT,
    customer_code VARCHAR(100),

    gender VARCHAR(50),
    age INTEGER,
    province VARCHAR(100),
    region VARCHAR(100),

    registered_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (source_customer_id)
);


-- ============================================================
-- 5. FACT: INTERNAL SALES
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,

    order_id BIGINT NOT NULL,
    order_number VARCHAR(100),

    date_key INTEGER NOT NULL,
    product_key BIGINT NOT NULL,
    customer_key BIGINT,
    platform_key INTEGER,

    quantity INTEGER NOT NULL,

    unit_price NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    line_total NUMERIC(18,2),

    payment_method VARCHAR(100),
    sales_channel VARCHAR(100),
    order_status VARCHAR(100),

    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_sales_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_key)
        REFERENCES dim_product(product_key),

    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES dim_customer(customer_key),

    CONSTRAINT fk_sales_platform
        FOREIGN KEY (platform_key)
        REFERENCES dim_platform(platform_key),

    UNIQUE (order_id, product_key)
);


-- ============================================================
-- 6. FACT: EXTERNAL PRODUCT SNAPSHOT
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_external_product (
    external_fact_key BIGSERIAL PRIMARY KEY,

    date_key INTEGER NOT NULL,
    product_key BIGINT NOT NULL,
    platform_key INTEGER NOT NULL,

    currency VARCHAR(20),

    price NUMERIC(18,4),
    original_price NUMERIC(18,4),
    discount_percent NUMERIC(10,4),

    rating NUMERIC(5,2),
    review_count BIGINT,
    sold_count BIGINT,

    seller_name TEXT,
    product_url TEXT,

    collected_at TIMESTAMP,
    processed_at TIMESTAMP,

    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_external_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_external_product
        FOREIGN KEY (product_key)
        REFERENCES dim_product(product_key),

    CONSTRAINT fk_external_platform
        FOREIGN KEY (platform_key)
        REFERENCES dim_platform(platform_key),

    UNIQUE (
        date_key,
        product_key,
        platform_key
    )
);


-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
ON dim_date(year, month);

CREATE INDEX IF NOT EXISTS idx_dim_product_category
ON dim_product(category);

CREATE INDEX IF NOT EXISTS idx_dim_product_brand
ON dim_product(brand);

CREATE INDEX IF NOT EXISTS idx_dim_product_source
ON dim_product(source_system, source_product_id);

CREATE INDEX IF NOT EXISTS idx_dim_customer_region
ON dim_customer(region);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date
ON fact_sales(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product
ON fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
ON fact_sales(customer_key);

CREATE INDEX IF NOT EXISTS idx_external_date
ON fact_external_product(date_key);

CREATE INDEX IF NOT EXISTS idx_external_product
ON fact_external_product(product_key);

CREATE INDEX IF NOT EXISTS idx_external_platform
ON fact_external_product(platform_key);


-- ============================================================
-- DEFAULT PLATFORMS
-- ============================================================

INSERT INTO dim_platform (
    platform_name,
    platform_type
)
VALUES
    ('internal', 'internal'),
    ('shopee', 'marketplace'),
    ('lazada', 'marketplace'),
    ('amazon', 'marketplace')
ON CONFLICT (platform_name)
DO NOTHING;