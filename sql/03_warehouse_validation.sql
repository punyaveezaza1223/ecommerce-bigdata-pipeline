-- Read-only warehouse checks. Run after DAG 03 completes.
-- Result statuses are explicit: PASS means the rule is clean, WARNING means
-- a usable but incomplete field, and FAIL means a referential/integrity defect.

-- 1. Unique marketplace products by source and total.
SELECT
    source_system,
    COUNT(*) AS unique_products
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY source_system
UNION ALL
SELECT
    'TOTAL_MARKETPLACE' AS source_system,
    COUNT(*) AS unique_products
FROM dim_product
WHERE source_system <> 'internal'
ORDER BY source_system;

-- 2. Products added on each warehouse load day (dimension created_at).
SELECT
    created_at::date AS warehouse_load_date,
    source_system,
    COUNT(*) AS new_unique_products
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY created_at::date, source_system
ORDER BY warehouse_load_date DESC, source_system;

-- 3. Identity duplicates: this must return zero rows.
SELECT
    source_system,
    source_product_id,
    COUNT(*) AS duplicate_count
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY source_system, source_product_id
HAVING COUNT(*) > 1;

-- 4. Required identity/name null or blank checks: all counts must be zero.
SELECT
    source_system,
    COUNT(*) FILTER (WHERE source_product_id IS NULL OR BTRIM(source_product_id) = '')
        AS missing_product_id,
    COUNT(*) FILTER (WHERE product_name IS NULL OR BTRIM(product_name) = '')
        AS missing_product_name
FROM dim_product
WHERE source_system <> 'internal'
GROUP BY source_system
ORDER BY source_system;

-- 5. Processed warehouse facts by source/date. Compare with Spark ETL logs.
SELECT
    p.platform_name AS source_system,
    d.full_date AS observation_date,
    COUNT(*) AS warehouse_rows
FROM fact_external_product f
JOIN dim_platform p ON p.platform_key = f.platform_key
JOIN dim_date d ON d.date_key = f.date_key
WHERE p.platform_type <> 'internal'
GROUP BY p.platform_name, d.full_date
ORDER BY d.full_date DESC, p.platform_name;

-- 6. Progress toward the presentation targets (unique marketplace products).
SELECT
    COUNT(*) AS unique_marketplace_products,
    500000 - COUNT(*) AS remaining_to_500k,
    1000000 - COUNT(*) AS remaining_to_1m
FROM dim_product
WHERE source_system <> 'internal';

-- 7. One row per fact platform with integrity and readiness checks.
WITH platform_quality AS (
    SELECT
        p.platform_name,
        COUNT(f.*) AS fact_rows,
        COUNT(DISTINCT f.product_key) AS unique_products,
        COUNT(*) FILTER (WHERE f.external_fact_key IS NOT NULL AND f.date_key IS NULL) AS null_date_keys,
        COUNT(*) FILTER (WHERE f.external_fact_key IS NOT NULL AND f.price IS NULL) AS null_prices,
        COUNT(*) FILTER (WHERE f.external_fact_key IS NOT NULL AND f.price < 0) AS invalid_prices,
        COUNT(*) FILTER (WHERE f.external_fact_key IS NOT NULL AND NULLIF(BTRIM(f.currency), '') IS NULL) AS missing_currency,
        MIN(d.full_date) AS min_observation_date,
        MAX(d.full_date) AS max_observation_date,
        COUNT(*) FILTER (WHERE f.external_fact_key IS NOT NULL AND (dp.product_key IS NULL OR dd.date_key IS NULL)) AS orphan_foreign_keys
    FROM dim_platform p
    LEFT JOIN fact_external_product f ON f.platform_key = p.platform_key
    LEFT JOIN dim_product dp ON dp.product_key = f.product_key
    LEFT JOIN dim_date dd ON dd.date_key = f.date_key
    LEFT JOIN dim_date d ON d.date_key = f.date_key
    WHERE p.platform_type <> 'internal'
    GROUP BY p.platform_name
)
SELECT
    platform_name,
    fact_rows,
    unique_products,
    null_date_keys,
    null_prices,
    invalid_prices,
    missing_currency,
    orphan_foreign_keys,
    min_observation_date,
    max_observation_date,
    CASE
        WHEN orphan_foreign_keys > 0 OR null_date_keys > 0 OR invalid_prices > 0 THEN 'FAIL'
        WHEN fact_rows = 0 OR null_prices > 0 OR missing_currency > 0 THEN 'WARNING'
        ELSE 'PASS'
    END AS status
FROM platform_quality
ORDER BY platform_name;

-- 8. Duplicate business fact keys: must return zero rows.
SELECT date_key, product_key, platform_key, COUNT(*) AS duplicate_count
FROM fact_external_product
GROUP BY date_key, product_key, platform_key
HAVING COUNT(*) > 1;
