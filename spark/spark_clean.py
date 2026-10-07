import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    trim,
    when,
    current_timestamp,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import DoubleType, LongType, StringType


# ============================================================
# CONFIG
# ============================================================

EBAY_PATH = "/opt/spark/data/raw/external/ebay"
MERCADO_LIBRE_PATH = "/opt/spark/data/raw/external/mercado_libre"
RAKUTEN_PATH = "/opt/spark/data/raw/external/rakuten"

PRODUCTION_OUTPUT_PATH = "/opt/spark/data/processed/products/external_products"
OUTPUT_PATH = os.getenv("SPARK_PRODUCTS_OUTPUT_PATH", "").strip() or PRODUCTION_OUTPUT_PATH


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceExternalProductsETL")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# COMMON COLUMNS
# ============================================================

COMMON_COLUMNS = [
    "platform",
    "external_product_id",
    "product_name",
    "category",
    "brand",
    "currency",
    "price",
    "original_price",
    "discount_percent",
    "rating",
    "review_count",
    "sold_count",
    "seller_name",
    "product_url",
    "image_url",
    "collected_at",
    "source_updated_at",
    "ingestion_timestamp",
    "raw_source",
]


# ============================================================
# STANDARDIZE FUNCTION
# ============================================================

def standardize(df, platform_name):

    # Add missing columns if necessary
    for column_name in COMMON_COLUMNS:
        if column_name not in df.columns:
            df = df.withColumn(
                column_name,
                lit(None)
            )

    df = df.select(*COMMON_COLUMNS)

    # --------------------------------------------------------
    # STRING CLEANING
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "platform",
            lower(trim(col("platform").cast(StringType())))
        )
        .withColumn(
            "external_product_id",
            trim(col("external_product_id").cast(StringType()))
        )
        .withColumn(
            "product_name",
            trim(col("product_name").cast(StringType()))
        )
        .withColumn(
            "category",
            trim(col("category").cast(StringType()))
        )
        .withColumn(
            "brand",
            trim(col("brand").cast(StringType()))
        )
        .withColumn(
            "currency",
            trim(col("currency").cast(StringType()))
        )
        .withColumn(
            "seller_name",
            trim(col("seller_name").cast(StringType()))
        )
        .withColumn(
            "product_url",
            trim(col("product_url").cast(StringType()))
        )
        .withColumn(
            "image_url",
            trim(col("image_url").cast(StringType()))
        )
        .withColumn(
            "raw_source",
            trim(col("raw_source").cast(StringType()))
        )
    )

    # --------------------------------------------------------
    # NUMERIC TYPES
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "price",
            col("price").cast(DoubleType())
        )
        .withColumn(
            "original_price",
            col("original_price").cast(DoubleType())
        )
        .withColumn(
            "discount_percent",
            col("discount_percent").cast(DoubleType())
        )
        .withColumn(
            "rating",
            col("rating").cast(DoubleType())
        )
        .withColumn(
            "review_count",
            col("review_count").cast(LongType())
        )
        .withColumn(
            "sold_count",
            col("sold_count").cast(LongType())
        )
    )

    # --------------------------------------------------------
    # TIMESTAMP
    # --------------------------------------------------------

    df = df.withColumn(
        "collected_at",
        to_timestamp(col("collected_at"))
    )

    df = df.withColumn(
        "source_updated_at",
        to_timestamp(col("source_updated_at"))
    )

    df = df.withColumn(
        "ingestion_timestamp",
        to_timestamp(col("ingestion_timestamp"))
    )

    # observation_date is the daily snapshot date
    df = df.withColumn(
        "observation_date",
        to_date(col("collected_at"))
    )

    # --------------------------------------------------------
    # BASIC DATA QUALITY
    # --------------------------------------------------------

    df = df.withColumn(
        "price",
        when(
            col("price") < 0,
            None
        ).otherwise(col("price"))
    )

    df = df.withColumn(
        "original_price",
        when(
            col("original_price") < 0,
            None
        ).otherwise(col("original_price"))
    )

    df = df.withColumn(
        "rating",
        when(
            (col("rating") < 0)
            | (col("rating") > 5),
            None
        ).otherwise(col("rating"))
    )

    df = df.withColumn(
        "review_count",
        when(
            col("review_count") < 0,
            None
        ).otherwise(col("review_count"))
    )

    df = df.withColumn(
        "sold_count",
        when(
            col("sold_count") < 0,
            None
        ).otherwise(col("sold_count"))
    )

    # Ensure platform is populated
    df = df.withColumn(
        "platform",
        when(
            col("platform").isNull()
            | (col("platform") == ""),
            lit(platform_name)
        ).otherwise(col("platform"))
    )

    return df


def read_optional_source(path, platform_name):
    """Read a Raw Parquet source when it is present and contains Parquet data."""
    source_path = Path(path)
    if not source_path.is_dir():
        print(f"Skipping {platform_name}: input path does not exist")
        return None

    if not any(source_path.rglob("*.parquet")):
        print(f"Skipping {platform_name}: no Parquet input files found")
        return None

    try:
        return spark.read.parquet(path)
    except Exception:
        print(f"Skipping {platform_name}: Parquet input could not be read")
        return None


# ============================================================
# READ RAW DATA
# ============================================================

print("=" * 60)
print("SPARK EXTERNAL PRODUCTS ETL")
print("=" * 60)

SOURCE_CONFIGS = [
    ("ebay", EBAY_PATH),
    ("mercado_libre", MERCADO_LIBRE_PATH),
    ("rakuten", RAKUTEN_PATH),
]

available_sources = []
print("\nRAW COUNTS")
print("-" * 60)
for platform_name, input_path in SOURCE_CONFIGS:
    source_df = read_optional_source(input_path, platform_name)
    if source_df is None:
        continue

    raw_count = source_df.count()
    print(f"{platform_name:<15}: {raw_count:,}")
    available_sources.append((platform_name, standardize(source_df, platform_name)))

if not available_sources:
    raise RuntimeError("No readable external Raw Parquet sources were found")


# ============================================================
# UNION
# ============================================================

print("Combining platforms...")

combined_df = available_sources[0][1]
for _, source_df in available_sources[1:]:
    combined_df = combined_df.unionByName(source_df, allowMissingColumns=True)


# ============================================================
# REMOVE INVALID CORE RECORDS
# ============================================================

combined_df = combined_df.filter(
    col("external_product_id").isNotNull()
    & col("product_name").isNotNull()
    & (trim(col("external_product_id")) != "")
    & (trim(col("product_name")) != "")
    & col("observation_date").isNotNull()
)


# ============================================================
# DEDUPLICATION
#
# IMPORTANT:
# Keep one product snapshot PER DAY.
#
# Same product on different dates must remain in the dataset
# so that price history can be analysed later.
# ============================================================

before_dedup = combined_df.count()

combined_df = combined_df.dropDuplicates(
    [
        "platform",
        "external_product_id",
        "observation_date",
    ]
)

after_dedup = combined_df.count()

duplicates_removed = (
    before_dedup
    - after_dedup
)


# ============================================================
# ADD ETL TIMESTAMP
# ============================================================

combined_df = combined_df.withColumn(
    "processed_at",
    current_timestamp()
)


# ============================================================
# DATA QUALITY SUMMARY
# ============================================================

rows_with_price = combined_df.filter(
    col("price").isNotNull()
).count()

rows_missing_price = combined_df.filter(
    col("price").isNull()
).count()

snapshot_days = (
    combined_df
    .select("observation_date")
    .distinct()
    .count()
)


print("\n" + "=" * 60)
print("ETL SUMMARY")
print("=" * 60)

print(
    f"Rows before dedup : {before_dedup:,}"
)

print(
    f"Duplicates removed: {duplicates_removed:,}"
)

print(
    f"Final rows        : {after_dedup:,}"
)

print(
    f"Rows with price   : {rows_with_price:,}"
)

print(
    f"Missing price     : {rows_missing_price:,}"
)

print(
    f"Snapshot days     : {snapshot_days:,}"
)


# ============================================================
# PLATFORM SUMMARY
# ============================================================

print("\nPLATFORM SUMMARY")
print("-" * 60)

combined_df.groupBy(
    "platform"
).count().orderBy(
    "platform"
).show(
    truncate=False
)


# ============================================================
# SNAPSHOT SUMMARY
# ============================================================

print("\nPLATFORM SNAPSHOT SUMMARY")
print("-" * 60)

(
    combined_df
    .groupBy("platform", "observation_date")
    .count()
    .orderBy("platform", "observation_date")
    .show(40, truncate=False)
)


# ============================================================
# SCHEMA
# ============================================================

print("\nFINAL SCHEMA")
print("-" * 60)

combined_df.printSchema()


# ============================================================
# SAMPLE
# ============================================================

print("\nSAMPLE DATA")
print("-" * 60)

combined_df.select(
    "platform",
    "external_product_id",
    "product_name",
    "observation_date",
    "currency",
    "price",
    "rating",
    "review_count",
).show(
    10,
    truncate=40
)


# ============================================================
# WRITE PROCESSED DATA
# ============================================================

print(
    f"\nWriting processed data to: {OUTPUT_PATH}"
)

(
    combined_df
    .write
    .mode("overwrite")
    .partitionBy("platform")
    .parquet(OUTPUT_PATH)
)


print("\n" + "=" * 60)
print("SPARK ETL COMPLETED")
print("=" * 60)

spark.stop()
