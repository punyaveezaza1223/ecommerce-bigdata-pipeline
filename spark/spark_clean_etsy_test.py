
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
# E-COMMERCE SPARK ETL - ETSY INTEGRATION TEST
# eBay + Rakuten + StockX + Etsy
# Writes ONLY to a separate test output directory.
# ============================================================

EBAY_PATH = "/opt/spark/data/raw/external/ebay"
RAKUTEN_PATH = "/opt/spark/data/raw/external/rakuten"
STOCKX_PATH = "/opt/spark/data/raw/external/stockx"
ETSY_PATH = "/opt/spark/data/raw/external/etsy"

PRODUCTION_OUTPUT_PATH = (
    "/opt/spark/data/processed/products/external_products"
)

OUTPUT_PATH = os.getenv(
    "SPARK_PRODUCTS_OUTPUT_PATH",
    "/opt/spark/data/processed_test/etsy_integration"
).strip()

SAFE_TEST_ROOT = Path(
    "/opt/spark/data/processed_test"
).resolve()

resolved_output = Path(OUTPUT_PATH).resolve()

if (
    resolved_output == SAFE_TEST_ROOT
    or SAFE_TEST_ROOT not in resolved_output.parents
    or resolved_output == Path(PRODUCTION_OUTPUT_PATH).resolve()
):
    raise RuntimeError(
        "Unsafe output path. Use a subdirectory of "
        "/opt/spark/data/processed_test/"
    )


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceExternalProductsEtsyTest")
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
# STANDARDIZE
# ============================================================

def standardize(df, platform_name):

    # Handle missing columns in Etsy and other sources
    for column_name in COMMON_COLUMNS:
        if column_name not in df.columns:
            df = df.withColumn(
                column_name,
                lit(None)
            )

    df = df.select(*COMMON_COLUMNS)

    # String normalization
    string_columns = [
        "platform",
        "external_product_id",
        "product_name",
        "category",
        "brand",
        "currency",
        "seller_name",
        "product_url",
        "image_url",
        "raw_source",
    ]

    for column_name in string_columns:
        df = df.withColumn(
            column_name,
            trim(col(column_name).cast(StringType()))
        )

    df = df.withColumn(
        "platform",
        lower(col("platform"))
    )

    df = df.withColumn(
        "platform",
        when(
            col("platform").isNull()
            | (col("platform") == ""),
            lit(platform_name)
        ).otherwise(col("platform"))
    )

    # Numeric normalization
    numeric_types = {
        "price": DoubleType(),
        "original_price": DoubleType(),
        "discount_percent": DoubleType(),
        "rating": DoubleType(),
        "review_count": LongType(),
        "sold_count": LongType(),
    }

    for column_name, data_type in numeric_types.items():
        df = df.withColumn(
            column_name,
            col(column_name).cast(data_type)
        )

    # Timestamps
    for column_name in [
        "collected_at",
        "source_updated_at",
        "ingestion_timestamp",
    ]:
        df = df.withColumn(
            column_name,
            to_timestamp(col(column_name))
        )

    df = df.withColumn(
        "observation_date",
        to_date(col("collected_at"))
    )

    # Data quality
    for column_name in [
        "price",
        "original_price",
        "review_count",
        "sold_count",
    ]:
        df = df.withColumn(
            column_name,
            when(
                col(column_name) < 0,
                lit(None)
            ).otherwise(col(column_name))
        )

    df = df.withColumn(
        "rating",
        when(
            (col("rating") < 0)
            | (col("rating") > 5),
            lit(None)
        ).otherwise(col("rating"))
    )

    return df


# ============================================================
# READ RAW PARQUET
# Supports both files and Spark Parquet directories
# ============================================================

def read_optional_source(path, platform_name):

    source_path = Path(path)

    if not source_path.is_dir():
        print(
            f"Skipping {platform_name}: directory not found"
        )
        return None

    parquet_paths = sorted(
        item
        for item in source_path.glob("*.parquet")
        if item.is_file() or item.is_dir()
    )

    if not parquet_paths:
        print(
            f"Skipping {platform_name}: no Parquet inputs"
        )
        return None

    print(
        f"{platform_name}: {len(parquet_paths)} "
        "Parquet inputs found"
    )

    frames = []

    # Read each snapshot independently to handle
    # possible differences between source schemas.
    for parquet_path in parquet_paths:
        part = spark.read.parquet(str(parquet_path))
        frames.append(
            standardize(part, platform_name)
        )

    result = frames[0]

    for part in frames[1:]:
        result = result.unionByName(
            part,
            allowMissingColumns=True
        )

    return result


# ============================================================
# MAIN ETL
# ============================================================

def main():

    source_configs = [
        ("ebay", EBAY_PATH),
        ("rakuten", RAKUTEN_PATH),
        ("stockx", STOCKX_PATH),
        ("etsy", ETSY_PATH),
    ]

    available_sources = []

    print("=" * 60)
    print("SPARK EXTERNAL PRODUCTS ETL - ETSY TEST")
    print("=" * 60)

    print("\nRAW COUNTS")
    print("-" * 60)

    for platform_name, input_path in source_configs:

        source_df = read_optional_source(
            input_path,
            platform_name
        )

        if source_df is None:
            if platform_name == "etsy":
                raise RuntimeError(
                    "Etsy source missing; test cannot continue"
                )
            continue

        raw_count = source_df.count()

        print(
            f"{platform_name:<15}: {raw_count:,}"
        )

        available_sources.append(source_df)

    if not available_sources:
        raise RuntimeError(
            "No readable Raw Parquet sources found"
        )

    # ----------------------------------------
    # UNION
    # ----------------------------------------

    print("\nCombining marketplaces...")

    combined_df = available_sources[0]

    for source_df in available_sources[1:]:
        combined_df = combined_df.unionByName(
            source_df,
            allowMissingColumns=True
        )

    # ----------------------------------------
    # VALIDATION
    # ----------------------------------------

    combined_df = combined_df.filter(
        col("external_product_id").isNotNull()
        & col("product_name").isNotNull()
        & (trim(col("external_product_id")) != "")
        & (trim(col("product_name")) != "")
        & col("observation_date").isNotNull()
    )

    before_dedup = combined_df.count()

    # One product snapshot per platform per day
    combined_df = combined_df.dropDuplicates(
        [
            "platform",
            "external_product_id",
            "observation_date",
        ]
    )

    after_dedup = combined_df.count()
    duplicates_removed = before_dedup - after_dedup

    combined_df = combined_df.withColumn(
        "processed_at",
        current_timestamp()
    )

    # ----------------------------------------
    # DATA QUALITY REPORT
    # ----------------------------------------

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

    print(f"Rows before dedup : {before_dedup:,}")
    print(f"Duplicates removed: {duplicates_removed:,}")
    print(f"Final rows        : {after_dedup:,}")
    print(f"Rows with price   : {rows_with_price:,}")
    print(f"Missing price     : {rows_missing_price:,}")
    print(f"Snapshot days     : {snapshot_days:,}")

    print("\nPLATFORM SUMMARY")
    print("-" * 60)

    combined_df.groupBy(
        "platform"
    ).count().orderBy(
        "platform"
    ).show(truncate=False)

    print("\nPLATFORM SNAPSHOT SUMMARY")
    print("-" * 60)

    (
        combined_df
        .groupBy("platform", "observation_date")
        .count()
        .orderBy("platform", "observation_date")
        .show(40, truncate=False)
    )

    print("\nFINAL SCHEMA")
    combined_df.printSchema()

    print("\nSAMPLE DATA")
    combined_df.select(
        "platform",
        "external_product_id",
        "product_name",
        "observation_date",
        "currency",
        "price",
        "rating",
        "review_count",
    ).show(10, truncate=40)

    # ----------------------------------------
    # WRITE TO TEST DIRECTORY ONLY
    # ----------------------------------------

    print(f"\nWriting TEST data to: {OUTPUT_PATH}")

    (
        combined_df
        .write
        .mode("overwrite")
        .partitionBy("platform")
        .parquet(OUTPUT_PATH)
    )

    print("\n" + "=" * 60)
    print("SPARK ETSY INTEGRATION TEST COMPLETED")
    print("=" * 60)


try:
    main()
finally:
    spark.stop()
