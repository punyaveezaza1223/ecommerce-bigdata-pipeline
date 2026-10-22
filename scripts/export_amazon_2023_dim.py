from pathlib import Path
from pyspark.sql import SparkSession, functions as F, Window

BASE = Path('/workspace/data/raw/historical/amazon_2023_parquet')
OUT = Path('/workspace/data/processed_test/amazon_2023_dim_export_v1')
if OUT.exists():
    raise SystemExit(f'STOP: export destination already exists: {OUT} (no overwrite)')
for category in ('raw_meta_All_Beauty', 'raw_meta_Electronics'):
    folder = BASE / category
    if not folder.exists() or not list(folder.glob('*.parquet')):
        raise SystemExit(f'STOP: metadata missing: {folder}')

spark = (SparkSession.builder.master('local[2]')
         .appName('Amazon2023HistoricalDimExport')
         .config('spark.sql.shuffle.partitions', '16')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    pieces = []
    for category in ('raw_meta_All_Beauty', 'raw_meta_Electronics'):
        df = spark.read.parquet(str(BASE / category / '*.parquet'))
        needed = ('parent_asin', 'title')
        missing = [c for c in needed if c not in df.columns]
        if missing:
            raise RuntimeError(f'{category}: missing columns {missing}')
        def col_or_null(name):
            return F.col(name).cast('string') if name in df.columns else F.lit(None).cast('string')
        # Avoid artificial dates and prices; retain only metadata for dim_product.
        category_text = col_or_null('main_category')
        if 'categories' in df.columns:
            category_text = F.coalesce(category_text, F.to_json(F.col('categories')))
        pieces.append(df.select(
            F.trim(F.col('parent_asin').cast('string')).alias('source_product_id'),
            F.trim(F.col('title').cast('string')).alias('product_name'),
            category_text.alias('category'),
            col_or_null('store').alias('brand')
        ))
    products = pieces[0].unionByName(pieces[1])
    products = products.filter(
        F.col('source_product_id').isNotNull() &
        (F.length('source_product_id') > 0) &
        (F.length('source_product_id') <= 255) &
        F.col('product_name').isNotNull() &
        (F.length('product_name') > 0)
    )
    # Consistent selection if a parent_asin is present more than once.
    window = Window.partitionBy('source_product_id').orderBy(
        F.length(F.col('product_name')).desc(), F.col('product_name').asc())
    products = (products.withColumn('_rn', F.row_number().over(window))
                .filter(F.col('_rn') == 1).drop('_rn'))
    for name in ('source_product_id', 'product_name', 'category', 'brand'):
        products = products.withColumn(name, F.regexp_replace(F.col(name), '\\u0000', ''))
    count = products.count()
    print('VALID_UNIQUE_PRODUCTS_TO_EXPORT:', count, flush=True)
    if count < 1000000:
        raise RuntimeError('Expected >=1,000,000 clean products. Aborting before write; inspect source fields.')
    (products.repartition(8)
     .write.mode('errorifexists')
     .option('header', 'true')
     .option('quoteAll', 'true')
     .option('escape', '"')
     .option('nullValue', '\\N')
     .csv(str(OUT)))
    print('EXPORT_SUCCESS:', OUT, flush=True)
finally:
    spark.stop()
