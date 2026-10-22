-- Power BI read models. These views do not modify source tables.

CREATE OR REPLACE VIEW vw_powerbi_products_by_marketplace AS
SELECT
    source_system AS marketplace,
    COUNT(*) AS unique_products,
    MIN(created_at) AS first_loaded_at,
    MAX(created_at) AS latest_loaded_at
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY source_system;

CREATE OR REPLACE VIEW vw_powerbi_products_by_category AS
SELECT
    source_system AS marketplace,
    COALESCE(NULLIF(BTRIM(category), ''), 'Uncategorized') AS category,
    COUNT(*) AS unique_products
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY source_system, COALESCE(NULLIF(BTRIM(category), ''), 'Uncategorized');

CREATE OR REPLACE VIEW vw_powerbi_price_distribution AS
SELECT
    p.platform_name AS marketplace,
    f.currency,
    CASE
        WHEN f.price IS NULL THEN 'Missing price'
        WHEN f.price < 25 THEN '< 25'
        WHEN f.price < 50 THEN '25-49.99'
        WHEN f.price < 100 THEN '50-99.99'
        WHEN f.price < 250 THEN '100-249.99'
        WHEN f.price < 500 THEN '250-499.99'
        ELSE '500+'
    END AS price_band,
    COUNT(*) AS observations
FROM fact_external_product f
JOIN dim_platform p ON p.platform_key = f.platform_key
WHERE p.platform_type <> 'internal'
GROUP BY p.platform_name, f.currency,
    CASE WHEN f.price IS NULL THEN 'Missing price' WHEN f.price < 25 THEN '< 25'
         WHEN f.price < 50 THEN '25-49.99' WHEN f.price < 100 THEN '50-99.99'
         WHEN f.price < 250 THEN '100-249.99' WHEN f.price < 500 THEN '250-499.99' ELSE '500+' END;

CREATE OR REPLACE VIEW vw_powerbi_product_growth AS
SELECT
    created_at::date AS ingestion_date,
    source_system AS marketplace,
    COUNT(*) AS new_unique_products
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY created_at::date, source_system;

CREATE OR REPLACE VIEW vw_powerbi_data_quality AS
SELECT
    source_system AS marketplace,
    COUNT(*) AS total_products,
    COUNT(*) FILTER (WHERE source_product_id IS NULL OR BTRIM(source_product_id) = '') AS missing_product_id,
    COUNT(*) FILTER (WHERE product_name IS NULL OR BTRIM(product_name) = '') AS missing_product_name,
    COUNT(*) - COUNT(DISTINCT source_product_id) AS duplicate_product_ids
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY source_system;

-- Detail-level, fact-led read model for Power BI.  Keep currency in the
-- grain: prices from different currencies must never be averaged together.
CREATE OR REPLACE VIEW vw_powerbi_marketplace_snapshot AS
SELECT
    p.platform_name AS marketplace,
    d.full_date AS observation_date,
    f.currency,
    pr.source_product_id,
    pr.product_name,
    pr.category,
    pr.brand,
    f.price,
    f.original_price,
    f.discount_percent,
    f.rating,
    f.review_count,
    f.sold_count,
    f.seller_name,
    f.product_url,
    f.collected_at,
    f.processed_at,
    f.loaded_at
FROM fact_external_product f
JOIN dim_platform p ON p.platform_key = f.platform_key
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_product pr ON pr.product_key = f.product_key
WHERE p.platform_type <> 'internal';
