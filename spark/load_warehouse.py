import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date,
)

# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("Load Warehouse External Product")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Config
# ============================================================

PRODUCTION_INPUT_PATH = "/opt/spark/data/processed/products/external_products"
INPUT_PATH = os.getenv("WAREHOUSE_INPUT_PATH", "").strip() or PRODUCTION_INPUT_PATH

def required_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "warehouse-postgres").strip()
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432").strip()
POSTGRES_DB = required_env("POSTGRES_DB")
POSTGRES_USER = required_env("POSTGRES_USER")
POSTGRES_PASSWORD = required_env("POSTGRES_PASSWORD")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", POSTGRES_DB).strip() or POSTGRES_DB

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{WAREHOUSE_DB}"

PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}
# ============================================================
# Helper
# ============================================================

def read_table(table_name):
    return (
        spark.read
        .jdbc(
            JDBC_URL,
            table_name,
            properties=PROPERTIES,
        )
    )


# ============================================================
# Read processed data
# ============================================================

print("=" * 60)
print("READING SPARK PROCESSED DATA")
print("=" * 60)

df = spark.read.parquet(INPUT_PATH)

raw_count = df.count()

print(f"Loaded rows: {raw_count:,}")

# ต้องมี product id และ platform
df = df.filter(
    col("platform").isNotNull()
    & col("external_product_id").isNotNull()
)

# วันที่ observation ของแต่ละ snapshot
df = df.withColumn(
    "observation_date",
    to_date(col("observation_date"))
)

df = df.filter(
    col("observation_date").isNotNull()
)

print(f"Valid rows : {df.count():,}")


# ============================================================
# Validate date dimension before any warehouse write
# ============================================================

input_dates = (
    df.select(
        col("observation_date").alias("full_date")
    )
    .distinct()
)

warehouse_dates = (
    read_table("dim_date")
    .select(
        to_date(col("full_date")).alias("full_date")
    )
    .distinct()
)

missing_dates = (
    input_dates
    .join(
        warehouse_dates,
        on="full_date",
        how="left_anti",
    )
    .orderBy("full_date")
    .collect()
)

if missing_dates:
    missing_date_values = ", ".join(
        row["full_date"].isoformat()
        for row in missing_dates
    )
    raise RuntimeError(
        "Missing observation dates in dim_date: "
        f"{missing_date_values}"
    )


# ============================================================
# DIM PLATFORM
# ============================================================

print("\n" + "=" * 60)
print("DIM PLATFORM")
print("=" * 60)

source_platforms = (
    df.select(
        col("platform").alias("platform_name")
    )
    .filter(col("platform_name").isNotNull())
    .distinct()
)

existing_platforms = (
    read_table("dim_platform")
    .select("platform_name")
)

new_platforms = (
    source_platforms
    .join(
        existing_platforms,
        on="platform_name",
        how="left_anti",
    )
    .withColumn(
        "platform_type",
        col("platform_name")
    )
)

# ให้ external platforms เป็น marketplace
new_platforms = new_platforms.withColumn(
    "platform_type",
    lit("marketplace")
)

new_platform_count = new_platforms.count()

print(f"New platforms: {new_platform_count:,}")

if new_platform_count > 0:
    (
        new_platforms
        .select(
            "platform_name",
            "platform_type",
        )
        .write
        .mode("append")
        .jdbc(
            JDBC_URL,
            "dim_platform",
            properties=PROPERTIES,
        )
    )


# ============================================================
# DIM PRODUCT
# ============================================================

print("\n" + "=" * 60)
print("DIM PRODUCT")
print("=" * 60)

source_products = (
    df.select(
        col("platform").alias("source_system"),
        col("external_product_id")
        .cast("string")
        .alias("source_product_id"),
        col("product_name"),
        col("category"),
        col("brand"),
        lit(None).cast("string").alias("sku"),
    )
    .filter(
        col("source_system").isNotNull()
        & col("source_product_id").isNotNull()
        & col("product_name").isNotNull()
    )
    .dropDuplicates(
        [
            "source_system",
            "source_product_id",
        ]
    )
)

existing_products = (
    read_table("dim_product")
    .select(
        "source_system",
        "source_product_id",
    )
)

new_products = (
    source_products
    .join(
        existing_products,
        on=[
            "source_system",
            "source_product_id",
        ],
        how="left_anti",
    )
)

new_product_count = new_products.count()

print(f"New products: {new_product_count:,}")

if new_product_count > 0:
    (
        new_products
        .select(
            "source_system",
            "source_product_id",
            "product_name",
            "category",
            "brand",
            "sku",
        )
        .write
        .mode("append")
        .jdbc(
            JDBC_URL,
            "dim_product",
            properties=PROPERTIES,
        )
    )


# ============================================================
# Reload dimensions
# ============================================================

print("\nReloading warehouse dimensions...")

dim_platform = (
    read_table("dim_platform")
    .select(
        "platform_key",
        "platform_name",
    )
)

dim_product = (
    read_table("dim_product")
    .select(
        "product_key",
        "source_system",
        "source_product_id",
    )
)

dim_date = (
    read_table("dim_date")
    .select(
        "date_key",
        "full_date",
    )
    .withColumn(
        "full_date",
        to_date(col("full_date"))
    )
)


# ============================================================
# Build FACT EXTERNAL PRODUCT
# ============================================================

print("\n" + "=" * 60)
print("FACT EXTERNAL PRODUCT")
print("=" * 60)

fact_source = (
    df.alias("s")

    # platform -> platform_key
    .join(
        dim_platform.alias("p"),
        col("s.platform") == col("p.platform_name"),
        "left",
    )

    # product -> product_key
    .join(
        dim_product.alias("dp"),
        (
            (col("s.platform") == col("dp.source_system"))
            & (
                col("s.external_product_id").cast("string")
                == col("dp.source_product_id")
            )
        ),
        "left",
    )

    # date -> date_key
    .join(
        dim_date.alias("d"),
        col("s.observation_date") == col("d.full_date"),
        "left",
    )
)

unresolved_facts = fact_source.filter(
    col("d.date_key").isNull()
    | col("dp.product_key").isNull()
    | col("p.platform_key").isNull()
)

unresolved_fact_count = unresolved_facts.count()

if unresolved_fact_count > 0:
    raise RuntimeError(
        "Unable to resolve warehouse dimension keys for "
        f"{unresolved_fact_count} input record(s)"
    )

fact_source = (
    fact_source
    .select(
        col("d.date_key").alias("date_key"),
        col("dp.product_key").alias("product_key"),
        col("p.platform_key").alias("platform_key"),

        col("s.currency").alias("currency"),
        col("s.price").cast("decimal(18,4)").alias("price"),
        col("s.original_price")
        .cast("decimal(18,4)")
        .alias("original_price"),

        col("s.discount_percent")
        .cast("decimal(10,4)")
        .alias("discount_percent"),

        col("s.rating")
        .cast("decimal(5,2)")
        .alias("rating"),

        col("s.review_count")
        .cast("long")
        .alias("review_count"),

        col("s.sold_count")
        .cast("long")
        .alias("sold_count"),

        col("s.seller_name").alias("seller_name"),
        col("s.product_url").alias("product_url"),

        col("s.collected_at")
        .cast("timestamp")
        .alias("collected_at"),

        current_timestamp()
        .alias("processed_at"),
    )

    .dropDuplicates(
        [
            "date_key",
            "product_key",
            "platform_key",
        ]
    )
)


# ============================================================
# Remove facts already loaded
# ============================================================

existing_facts = (
    read_table("fact_external_product")
    .select(
        "date_key",
        "product_key",
        "platform_key",
    )
)

new_facts = (
    fact_source
    .join(
        existing_facts,
        on=[
            "date_key",
            "product_key",
            "platform_key",
        ],
        how="left_anti",
    )
)

fact_source_count = fact_source.count()
new_fact_count = new_facts.count()

print(f"Prepared facts : {fact_source_count:,}")
print(f"New facts      : {new_fact_count:,}")
print(
    f"Already loaded : "
    f"{fact_source_count - new_fact_count:,}"
)


# ============================================================
# Write facts
# ============================================================

if new_fact_count > 0:

    (
        new_facts
        .select(
            "date_key",
            "product_key",
            "platform_key",
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
            "processed_at",
        )
        .write
        .mode("append")
        .jdbc(
            JDBC_URL,
            "fact_external_product",
            properties=PROPERTIES,
        )
    )


# ============================================================
# Summary
# ============================================================

print("\n" + "=" * 60)
print("WAREHOUSE LOAD COMPLETED")
print("=" * 60)

print(f"Processed rows : {raw_count:,}")
print(f"New platforms  : {new_platform_count:,}")
print(f"New products   : {new_product_count:,}")
print(f"New facts      : {new_fact_count:,}")

print("=" * 60)

spark.stop()
