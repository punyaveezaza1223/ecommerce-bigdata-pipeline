-- Idempotent live-warehouse reconciliation. This creates no facts and changes
-- no existing rows; it only ensures every supported source has a platform key.
INSERT INTO dim_platform (platform_name, platform_type)
VALUES
    ('etsy', 'marketplace'),
    ('advice_th', 'marketplace'),
    ('amazon_2023', 'marketplace')
ON CONFLICT (platform_name) DO NOTHING;
