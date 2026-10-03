from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
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

INPUT_PATH = "/opt/spark/data/processed/products/external_products"

JDBC_URL = "jdbc:postgresql://warehouse-postgres:5432/ecommerce_warehouse"

PROPERTIES = {
    "user": "warehouse",
    "password": "warehouse123",
    "driver": "org.postgresql.Driver",
}

BEST_BUY_PLATFORM = "best_buy"
BEST_BUY_RETENTION_HOURS = 72


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


def execute_warehouse_sql(statement: str) -> int:
    """Execute warehouse cleanup SQL through the existing JDBC connection."""
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        PROPERTIES["user"],
        PROPERTIES["password"],
    )
    try:
        cursor = connection.createStatement()
        try:
            return cursor.executeUpdate(statement)
        finally:
            cursor.close()
    finally:
        connection.close()


def purge_expired_best_buy_content() -> tuple[int, int]:
    """Delete Best Buy content that exceeds its 72-hour retention window."""
    expired_facts = execute_warehouse_sql(
        """
        DELETE FROM fact_external_product AS fact
        USING dim_platform AS platform
        WHERE fact.platform_key = platform.platform_key
          AND platform.platform_name = 'best_buy'
          AND COALESCE(fact.collected_at, fact.loaded_at)
              < CURRENT_TIMESTAMP - INTERVAL '72 hours'
        """
    )
    expired_products = execute_warehouse_sql(
        """
        DELETE FROM dim_product AS product
        WHERE product.source_system = 'best_buy'
          AND NOT EXISTS (
              SELECT 1
              FROM fact_external_product AS fact
              WHERE fact.product_key = product.product_key
          )
        """
    )
    return expired_facts, expired_products


# ============================================================
# Read processed data
# ============================================================

print("=" * 60)
print("READING SPARK PROCESSED DATA")
print("=" * 60)

expired_fact_count, expired_product_count = purge_expired_best_buy_content()
print(f"Expired Best Buy facts removed    : {expired_fact_count:,}")
print(f"Expired Best Buy products removed : {expired_product_count:,}")

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

# Best Buy API content is not eligible for Warehouse storage after 72 hours.
df = df.filter(
    (col("platform") != BEST_BUY_PLATFORM)
    | (
        col("collected_at").cast("timestamp")
        >= expr(f"current_timestamp() - INTERVAL {BEST_BUY_RETENTION_HOURS} HOURS")
    )
)

print(f"Valid rows : {df.count():,}")


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
        "inner",
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
        "inner",
    )

    # date -> date_key
    .join(
        dim_date.alias("d"),
        col("s.observation_date") == col("d.full_date"),
        "inner",
    )

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
