from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path("/workspace")
SOURCE = ROOT / "data/raw/external/advice_th/advice_th_20261011_164306_300987.json"
OUTPUT = ROOT / "data/processed_test/advice_th_warehouse_input_v1"

if not SOURCE.is_file():
    raise RuntimeError(f"Source missing: {SOURCE}")

if OUTPUT.exists():
    raise RuntimeError(f"Output already exists: {OUTPUT}")

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("AdviceThailandSparkETL")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    raw = (
        spark.read
        .option("multiLine", "true")
        .json(str(SOURCE))
    )

    products = raw.select(F.explode("products").alias("p"))

    cleaned = products.select(
        F.col("p.source_system").alias("source_system"),
        F.trim(F.col("p.external_product_id")).alias("external_product_id"),
        F.trim(F.col("p.product_name")).alias("product_name"),
        F.col("p.category").alias("category"),
        F.col("p.price").cast("double").alias("price"),
        F.col("p.currency").alias("currency"),
        F.col("p.product_url").alias("product_url"),
        F.col("p.source_page").alias("source_page"),
        F.col("p.source_type").alias("source_type"),
        F.to_timestamp("p.observed_at_utc").alias("observed_at_utc"),
        F.col("p.price_list_last_update").alias("price_list_last_update")
    )

    valid = (
        cleaned
        .filter(F.col("source_system") == "advice_th")
        .filter(F.col("external_product_id").rlike(r"^A[0-9]+$"))
        .filter(F.col("product_name").isNotNull())
        .filter(F.length("product_name") > 0)
        .filter(F.col("price") > 0)
        .filter(F.col("currency") == "THB")
        .filter(F.col("observed_at_utc").isNotNull())
        .dropDuplicates(["source_system", "external_product_id"])
    )

    count = valid.count()
    print("RAW_PRODUCTS:", products.count(), flush=True)
    print("VALID_UNIQUE_PRODUCTS:", count, flush=True)

    if count != 196:
        raise RuntimeError(
            f"Expected 196 products, got {count}. "
            "Stopping without writing."
        )

    (
        valid.coalesce(1)
        .write
        .mode("errorifexists")
        .parquet(str(OUTPUT))
    )

    print("SPARK_ETL_SUCCESS:", OUTPUT, flush=True)
    print("PRODUCTS:", count, flush=True)

finally:
    spark.stop()
