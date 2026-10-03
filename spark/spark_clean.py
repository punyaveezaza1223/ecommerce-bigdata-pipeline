from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    trim,
    when,
    current_timestamp,
    expr,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import DoubleType, LongType, StringType


# ============================================================
# CONFIG
# ============================================================

EBAY_PATH = "/opt/spark/data/raw/external/ebay"
MERCADO_LIBRE_PATH = "/opt/spark/data/raw/external/mercado_libre"
BEST_BUY_PATH = "/opt/spark/data/raw/external/best_buy"

OUTPUT_PATH = "/opt/spark/data/processed/products/external_products"
BEST_BUY_PLATFORM = "best_buy"
BEST_BUY_RETENTION_HOURS = 72


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


# ============================================================
# READ RAW DATA
# ============================================================

print("=" * 60)
print("SPARK EXTERNAL PRODUCTS ETL")
print("=" * 60)

print("\nReading eBay...")
ebay_df = spark.read.parquet(EBAY_PATH)

print("Reading Mercado Libre...")
mercado_libre_df = spark.read.parquet(MERCADO_LIBRE_PATH)

print("Reading Best Buy...")
best_buy_df = spark.read.parquet(BEST_BUY_PATH)


# ============================================================
# RAW COUNTS
# ============================================================

ebay_raw_count = ebay_df.count()
mercado_libre_raw_count = mercado_libre_df.count()
best_buy_raw_count = best_buy_df.count()

print("\nRAW COUNTS")
print("-" * 60)

print(f"eBay          : {ebay_raw_count:,}")
print(f"Mercado Libre : {mercado_libre_raw_count:,}")
print(f"Best Buy      : {best_buy_raw_count:,}")


# ============================================================
# STANDARDIZE
# ============================================================

print("\nStandardizing schemas...")

ebay_clean = standardize(
    ebay_df,
    "ebay"
)

mercado_libre_clean = standardize(
    mercado_libre_df,
    "mercado_libre"
)

best_buy_clean = standardize(
    best_buy_df,
    "best_buy"
)


# ============================================================
# UNION
# ============================================================

print("Combining platforms...")

combined_df = (
    ebay_clean
    .unionByName(
        mercado_libre_clean,
        allowMissingColumns=True
    )
    .unionByName(
        best_buy_clean,
        allowMissingColumns=True
    )
)


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


# Best Buy API content cannot remain in the processed layer beyond 72 hours.
# The overwrite below removes expired Best Buy records from prior processed output.
before_best_buy_retention = combined_df.count()

combined_df = combined_df.filter(
    (col("platform") != BEST_BUY_PLATFORM)
    | (
        col("collected_at")
        >= expr(f"current_timestamp() - INTERVAL {BEST_BUY_RETENTION_HOURS} HOURS")
    )
)

after_best_buy_retention = combined_df.count()
best_buy_expired_removed = before_best_buy_retention - after_best_buy_retention


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
    f"Expired Best Buy removed: {best_buy_expired_removed:,}"
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
