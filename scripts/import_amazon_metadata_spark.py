#!/usr/bin/env python3
"""Transform official Amazon Reviews 2023 gzipped metadata into warehouse-compatible Parquet.
Writes only under processed_test/historical; never touches production outputs or DB.
"""
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               LongType, ArrayType)

ROOT = Path('/workspace')
DEFAULT_INPUT = ROOT / 'data/raw/historical/amazon_2023/meta_Amazon_Fashion.jsonl.gz'
DEFAULT_OUTPUT = ROOT / 'data/processed_test/historical_amazon_fashion'
SAFE_ROOT = (ROOT / 'data/processed_test').resolve()

SCHEMA = StructType([
    StructField('parent_asin', StringType()),
    StructField('title', StringType()),
    StructField('main_category', StringType()),
    StructField('categories', ArrayType(StringType())),
    StructField('price', StringType()),
    StructField('average_rating', DoubleType()),
    StructField('rating_number', LongType()),
    StructField('store', StringType()),
])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', default=str(DEFAULT_INPUT))
    ap.add_argument('--output', default=str(DEFAULT_OUTPUT))
    ap.add_argument('--max-products', type=int, default=200000,
                    help='Cap unique product rows for initial safe test')
    ap.add_argument('--partitions', type=int, default=4)
    args = ap.parse_args()
    input_path = Path(args.input)
    output = Path(args.output).resolve()
    if not input_path.is_file():
        raise SystemExit(f'Input missing: {input_path}')
    if output == SAFE_ROOT or SAFE_ROOT not in output.parents:
        raise SystemExit('Output must be inside /workspace/data/processed_test/')
    if output.exists():
        raise SystemExit(f'Output exists, refusing overwrite: {output}')
    if args.max_products < 1 or args.max_products > 1000000:
        raise SystemExit('--max-products must be 1..1000000')
    if not 1 <= args.partitions <= 16:
        raise SystemExit('--partitions must be 1..16')

    spark = (SparkSession.builder.master('local[2]')
             .appName('Amazon2023HistoricalMetadataETL')
             .config('spark.sql.shuffle.partitions', str(args.partitions))
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        # .gz JSONL can be read directly without extracting a huge text file.
        raw = spark.read.schema(SCHEMA).json(str(input_path))
        clean = (raw
            .select(
                F.lit('amazon_2023').alias('platform'),
                F.trim(F.col('parent_asin')).alias('external_product_id'),
                F.trim(F.col('title')).alias('product_name'),
                F.coalesce(F.element_at('categories', 1), F.col('main_category'))
                 .cast('string').alias('category'),
                F.lit(None).cast('string').alias('brand'),
                F.lit('USD').alias('currency'),
                F.when(F.trim(F.col('price')).rlike(r'^\d+(\.\d+)?$'),
                       F.col('price').cast('double')).otherwise(F.lit(None).cast('double'))
                 .alias('price'),
                F.col('average_rating').alias('rating'),
                F.col('rating_number').alias('review_count'),
                F.col('store').alias('seller_name'),
            )
            .filter(F.col('external_product_id').isNotNull()
                    & (F.length('external_product_id') > 0)
                    & F.col('product_name').isNotNull()
                    & (F.length('product_name') > 0))
            .dropDuplicates(['external_product_id']))
        # Limit AFTER dedup. Non-deterministic subset is fine for an initial test.
        clean = clean.limit(args.max_products)
        # 2023-09-30 is a HISTORICAL REFERENCE DATE for warehouse compatibility,
        # not the date when every item was collected or listed.
        df = (clean
              .withColumn('price', F.when(F.col('price') >= 0, F.col('price')))
              .withColumn('rating', F.when(F.col('rating').between(0, 5), F.col('rating')))
              .withColumn('review_count', F.when(F.col('review_count') >= 0, F.col('review_count')))
              .withColumn('original_price', F.lit(None).cast('double'))
              .withColumn('discount_percent', F.lit(None).cast('double'))
              .withColumn('sold_count', F.lit(None).cast('long'))
              .withColumn('sku', F.lit(None).cast('string'))
              .withColumn('image_url', F.lit(None).cast('string'))
              .withColumn('product_url', F.concat(F.lit('https://www.amazon.com/dp/'), F.col('external_product_id')))
              .withColumn('collected_at', F.lit(None).cast('timestamp'))
              .withColumn('source_updated_at', F.lit(None).cast('timestamp'))
              .withColumn('ingestion_timestamp', F.current_timestamp())
              .withColumn('observation_date', F.lit('2023-09-30').cast('date'))
              .withColumn('processed_at', F.current_timestamp())
              .withColumn('raw_source', F.lit('amazon_reviews_2023_historical_metadata')))
        count = df.count()
        print('VALID HISTORICAL PRODUCTS:', count)
        df.groupBy('platform').count().show(truncate=False)
        df.printSchema()
        if count == 0:
            raise RuntimeError('No valid products; nothing written')
        df.repartition(args.partitions).write.mode('errorifexists').parquet(str(output))
        print('HISTORICAL PARQUET CREATED:', output)
        print('ROWS:', count)
        print('DO NOT call this live Amazon data; source is historical 2023 metadata.')
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
