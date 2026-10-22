
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================
# E-COMMERCE LOCAL DATA LAKE
# Raw Parquet -> Bronze JSON -> Silver Parquet
# ============================================

BASE_DIR = Path("/workspace/data")

SOURCES = ["ebay", "rakuten", "stockx","etsy"]

SAMPLE_LIMIT = 100


def find_column(df, candidates):
    """Find the first existing column and return it as string."""

    for name in candidates:
        if name in df.columns:
            escaped = name.replace("`", "``")
            return F.col(f"`{escaped}`").cast("string")

    return F.lit(None).cast("string")


def process_source(spark, source):

    print(f"\n========== {source.upper()} ==========")

    # ----------------------------------------
    # STEP 1: READ RAW PARQUET
    # ----------------------------------------

    raw_folder = BASE_DIR / "raw" / "external" / source

    if not raw_folder.exists():
        print(f"SKIP: Folder not found: {raw_folder}")
        return

    raw_files = sorted(
        raw_folder.glob("*.parquet"),
        key=lambda file: file.stat().st_mtime,
        reverse=True
    )

    if not raw_files:
        print(f"SKIP: No Parquet files for {source}")
        return

    raw_file = raw_files[0]

    print(f"RAW FILE: {raw_file.name}")

    raw_df = (
        spark.read
        .parquet(str(raw_file))
        .limit(SAMPLE_LIMIT)
    )

    print(f"RAW ROWS: {raw_df.count()}")
    print(f"RAW COLUMNS: {raw_df.columns}")

    # Inspect StockX prices before transformation
    if source == "stockx":
        print("\nSTOCKX RAW PRICE SAMPLE:")

        if "price" in raw_df.columns:
            raw_df.select("price").show(
                10,
                truncate=False
            )
        else:
            print("WARNING: StockX has no price column")

    # ----------------------------------------
    # STEP 2: BRONZE LAYER (JSON)
    # ----------------------------------------

    # Demo JSON converted from original Raw Parquet.
    # Does not modify the original Raw files.

    bronze_path = (
        BASE_DIR
        / "lake"
        / "bronze"
        / source
        / "sample_json"
    )

    (
        raw_df.coalesce(1)
        .write
        .mode("overwrite")
        .json(str(bronze_path))
    )

    print(f"BRONZE CREATED: {bronze_path}")

    # ----------------------------------------
    # STEP 3: READ BRONZE WITH SPARK
    # ----------------------------------------

    bronze_df = spark.read.json(str(bronze_path))

    print(f"BRONZE ROWS: {bronze_df.count()}")

    # ----------------------------------------
    # STEP 4: TRANSFORM INTO SILVER
    # ----------------------------------------

    normalized_df = bronze_df.select(

        F.lit(source).alias("source_system"),

        find_column(
            bronze_df,
            [
                "external_product_id",
                "product_id",
                "external_id",
                "item_id",
                "itemCode",
                "id",
                "sku"
            ]
        ).alias("product_id"),

        find_column(
            bronze_df,
            [
                "product_name",
                "item_name",
                "itemName",
                "title",
                "name"
            ]
        ).alias("product_name"),

        find_column(
            bronze_df,
            [
                "price",
                "itemPrice",
                "current_price",
                "price_amount",
                "price_value"
            ]
        ).alias("price_text"),

        find_column(
            bronze_df,
            [
                "currency",
                "currency_code"
            ]
        ).alias("currency"),

        F.to_json(
            F.struct(
                *[
                    F.col(
                        f"`{name.replace('`', '``')}`"
                    )
                    for name in bronze_df.columns
                ]
            )
        ).alias("source_record_json")

    )

    silver_df = normalized_df.select(

        F.col("source_system"),

        F.col("product_id"),

        F.col("product_name"),

        F.expr(
            "try_cast(price_text AS DOUBLE)"
        ).alias("price"),

        F.col("currency"),

        F.col("source_record_json")

    )

    # ----------------------------------------
    # STEP 5: SILVER LAYER (PARQUET)
    # ----------------------------------------

    silver_path = (
        BASE_DIR
        / "lake"
        / "silver"
        / "products"
        / source
    )

    (
        silver_df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(silver_path))
    )

    print(f"SILVER CREATED: {silver_path}")

    # ----------------------------------------
    # STEP 6: DATA QUALITY CHECK
    # ----------------------------------------

    result_df = spark.read.parquet(str(silver_path))

    total_rows = result_df.count()

    valid_ids = result_df.filter(
        F.col("product_id").isNotNull()
    ).count()

    valid_names = result_df.filter(
        F.col("product_name").isNotNull()
    ).count()

    valid_prices = result_df.filter(
        F.col("price").isNotNull()
    ).count()

    print("\nDATA QUALITY REPORT")
    print("------------------------------")

    print(f"SOURCE: {source.upper()}")
    print(f"TOTAL ROWS: {total_rows}")
    print(f"VALID PRODUCT IDs: {valid_ids}")
    print(f"VALID PRODUCT NAMES: {valid_names}")
    print(f"VALID NUMERIC PRICES: {valid_prices}")

    print("\nSILVER DATA SAMPLE:")

    result_df.select(
        "source_system",
        "product_id",
        "product_name",
        "price",
        "currency"
    ).show(5, truncate=35)

    print(f"\nSUCCESS: {source.upper()}")


def main():

    spark = (
        SparkSession.builder
        .appName("EcommerceLocalDataLake")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        for source in SOURCES:
            process_source(spark, source)

        print("\n====================================")
        print("LOCAL DATA LAKE BUILD COMPLETED")
        print("====================================")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
