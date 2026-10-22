
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# ============================================================
# PREPARE ETSY-ONLY WAREHOUSE INPUT
# Read from successful ETL test output.
# Never overwrite production or existing test data.
# ============================================================

BASE_PATH = (
    "/opt/spark/data/processed_test/etsy_integration"
)
ETSY_PATH = f"{BASE_PATH}/platform=etsy"
OUTPUT_PATH = (
    "/opt/spark/data/processed_test/etsy_warehouse_input"
)

REQUIRED_COLUMNS = [
    "platform",
    "external_product_id",
    "product_name",
    "observation_date",
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
]

def main():
    if not Path(ETSY_PATH).is_dir():
        raise RuntimeError(
            f"Etsy partition not found: {ETSY_PATH}"
        )

    if Path(OUTPUT_PATH).exists():
        raise RuntimeError(
            "Output already exists. Stopping to avoid overwrite: "
            + OUTPUT_PATH
        )

    spark = (
        SparkSession.builder
        .appName("PrepareEtsyWarehouseInput")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # basePath preserves platform from platform=etsy
        df = (
            spark.read
            .option("basePath", BASE_PATH)
            .parquet(ETSY_PATH)
        )

        missing = [
            name for name in REQUIRED_COLUMNS
            if name not in df.columns
        ]

        if missing:
            raise RuntimeError(
                f"Missing required columns: {missing}"
            )

        # Reject any unexpected platform.
        invalid_platform_count = df.filter(
            col("platform").isNull()
            | (col("platform") != "etsy")
        ).count()

        if invalid_platform_count:
            raise RuntimeError(
                "Input contains non-Etsy records"
            )

        df = df.filter(
            col("external_product_id").isNotNull()
            & (trim(col("external_product_id")) != "")
            & col("product_name").isNotNull()
            & (trim(col("product_name")) != "")
            & col("observation_date").isNotNull()
        )

        df = df.dropDuplicates(
            ["platform", "external_product_id",
             "observation_date"]
        )

        count = df.count()

        print("\n==============================")
        print("ETSY WAREHOUSE INPUT CHECK")
        print("==============================")
        print("Valid Etsy snapshot rows:", count)

        df.groupBy("platform").count().show()

        if count != 20:
            raise RuntimeError(
                f"Expected 20 Etsy rows; found {count}. "
                "No output was written."
            )

        print("\nOUTPUT SCHEMA")
        df.printSchema()

        # No partitionBy: platform remains an actual column.
        (
            df.coalesce(1)
            .write
            .mode("errorifexists")
            .parquet(OUTPUT_PATH)
        )

        print("\nETSY WAREHOUSE INPUT CREATED")
        print("PATH:", OUTPUT_PATH)
        print("ROWS:", count)
        print("SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
