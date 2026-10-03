-- ============================================================
-- E-COMMERCE BIG DATA PIPELINE
-- Internal Source Database Schema
-- Database: ecommerce_internal
-- ============================================================


-- ============================================================
-- 1. PRODUCTS
-- ข้อมูลสินค้าภายในร้าน
-- ============================================================
CREATE TABLE IF NOT EXISTS products (
    product_id BIGSERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    brand VARCHAR(100),
    cost_price NUMERIC(12,2) NOT NULL,
    selling_price NUMERIC(12,2) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_product_cost
        CHECK (cost_price >= 0),

    CONSTRAINT chk_product_price
        CHECK (selling_price >= 0),

    CONSTRAINT chk_product_stock
        CHECK (stock_quantity >= 0)
);


-- ============================================================
-- 2. CUSTOMERS
-- ข้อมูลลูกค้า
-- ============================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_code VARCHAR(50) NOT NULL UNIQUE,
    gender VARCHAR(20),
    age INTEGER,
    province VARCHAR(100),
    region VARCHAR(50),
    registered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_customer_age
        CHECK (age IS NULL OR age BETWEEN 10 AND 120)
);


-- ============================================================
-- 3. ORDERS
-- ข้อมูลคำสั่งซื้อ
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGSERIAL PRIMARY KEY,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    customer_id BIGINT NOT NULL,

    order_date TIMESTAMP NOT NULL,
    payment_method VARCHAR(50),
    sales_channel VARCHAR(50),

    order_status VARCHAR(30) NOT NULL DEFAULT 'completed',

    total_amount NUMERIC(14,2) NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    CONSTRAINT chk_order_total
        CHECK (total_amount >= 0)
);


-- ============================================================
-- 4. ORDER ITEMS
-- รายการสินค้าภายในแต่ละ Order
-- ตารางนี้จะเป็น Transaction หลักและสามารถมีหลายล้านแถวได้
-- ============================================================
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,

    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    line_total NUMERIC(14,2) NOT NULL,

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id),

    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES products(product_id),

    CONSTRAINT chk_item_quantity
        CHECK (quantity > 0),

    CONSTRAINT chk_item_price
        CHECK (unit_price >= 0),

    CONSTRAINT chk_item_discount
        CHECK (discount_amount >= 0),

    CONSTRAINT chk_item_total
        CHECK (line_total >= 0)
);


-- ============================================================
-- INDEXES
-- สำหรับรองรับ Query ข้อมูลจำนวนมาก
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_products_category
    ON products(category);

CREATE INDEX IF NOT EXISTS idx_products_brand
    ON products(brand);

CREATE INDEX IF NOT EXISTS idx_customers_province
    ON customers(province);

CREATE INDEX IF NOT EXISTS idx_orders_customer
    ON orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_date
    ON orders(order_date);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders(order_status);

CREATE INDEX IF NOT EXISTS idx_order_items_order
    ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON order_items(product_id);