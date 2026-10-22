$ErrorActionPreference = 'Stop'
Set-Location 'C:\ecommerce-bigdata-pipeline'
$source = '.\data\processed_test\amazon_2023_dim_export_v1'
if (-not (Test-Path (Join-Path $source '_SUCCESS'))) {
    throw 'Spark export not finished: _SUCCESS missing. Do not import.'
}
$parts = @(Get-ChildItem $source -File -Filter 'part-*.csv' | Sort-Object Name)
if ($parts.Count -eq 0) { throw 'No exported CSV parts found.' }

$sql = @'
\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE amazon_import_stage (
    source_product_id text,
    product_name text,
    category text,
    brand text
) ON COMMIT DROP;
\copy amazon_import_stage (source_product_id,product_name,category,brand) FROM '/tmp/amazon_import_chunk.csv' WITH (FORMAT csv, HEADER true, NULL '\N');
INSERT INTO public.dim_product
    (source_system, source_product_id, product_name, category, brand)
SELECT 'amazon_2023', source_product_id, product_name,
       NULLIF(category, ''), NULLIF(brand, '')
FROM amazon_import_stage
WHERE source_product_id IS NOT NULL AND length(source_product_id) BETWEEN 1 AND 255
  AND product_name IS NOT NULL AND length(product_name) > 0
ON CONFLICT (source_system, source_product_id) DO NOTHING;
COMMIT;
'@
$sqlPath = Join-Path $env:TEMP ("amazon_2023_import_batch_{0}.sql" -f [guid]::NewGuid().ToString("N"))
# Avoid overriding a file of the same name outside the project.
if (Test-Path $sqlPath) { throw "Existing SQL script needs inspection: $sqlPath" }
[System.IO.File]::WriteAllText($sqlPath, $sql, (New-Object System.Text.UTF8Encoding($false)))
try {
    docker cp $sqlPath 'pipeline-warehouse-postgres:/tmp/amazon_2023_import_batch.sql'
    if ($LASTEXITCODE -ne 0) { throw 'Could not copy SQL script to PostgreSQL container' }
    foreach ($part in $parts) {
        $free = (Get-PSDrive C).Free
        if ($free -lt 15GB) { throw 'STOP: less than 15 GiB free on C:. No further import.' }
        Write-Host "IMPORTING: $($part.Name)" -ForegroundColor Cyan
        docker cp $part.FullName 'pipeline-warehouse-postgres:/tmp/amazon_import_chunk.csv'
        if ($LASTEXITCODE -ne 0) { throw 'docker cp failed; stopping import' }
        docker exec pipeline-warehouse-postgres psql -X -v ON_ERROR_STOP=1 -U warehouse -d ecommerce_warehouse -f /tmp/amazon_2023_import_batch.sql
        if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL import error. Previous batches are committed; this batch rolled back.' }
        Write-Host "COMMITTED: $($part.Name)" -ForegroundColor Green
    }
    Write-Host "DONE. Read-only verification:" -ForegroundColor Green
    docker exec pipeline-warehouse-postgres psql -X -U warehouse -d ecommerce_warehouse -c "SELECT source_system, count(*) FROM dim_product GROUP BY source_system ORDER BY count(*) DESC;"
}
finally {
    # Do not remove any files or container state; temporary copies are left for inspection.
    Write-Host 'Completed or stopped. Source Parquet and live warehouse rows were not overwritten.'
}
