
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path("/workspace/data")

SOURCES = ["ebay", "rakuten", "stockx","etsy"]


def main():

    spark = (
        SparkSession.builder
        .appName("EcommerceLakeGold")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Read Silver Parquet from each marketplace
        all_products = None

        for source in SOURCES:

            silver_path = (
                BASE_DIR
                / "lake"
                / "silver"
                / "products"
                / source
            )

            if not silver_path.exists():
                raise FileNotFoundError(
                    f"Silver data not found: {silver_path}"
                )

            df = spark.read.parquet(str(silver_path))

            if all_products is None:
                all_products = df
            else:
                all_products = all_products.unionByName(df)

        print("\nTOTAL SILVER ROWS:", all_products.count())

        # Group by source and currency to avoid
        # mixing USD and JPY in price averages
        gold_df = (
            all_products
            .groupBy("source_system", "currency")
            .agg(
                F.count("*").alias("total_rows"),

                F.countDistinct(
                    "product_id"
                ).alias("unique_products"),

                F.sum(
                    F.when(
                        F.col("price").isNotNull(),
                        1
                    ).otherwise(0)
                ).alias("products_with_price"),

                F.sum(
                    F.when(
                        F.col("price").isNull(),
                        1
                    ).otherwise(0)
                ).alias("products_missing_price"),

                F.round(
                    F.avg("price"),
                    2
                ).alias("average_price")
            )
        )

        # Gold Parquet
        gold_path = (
            BASE_DIR
            / "lake"
            / "gold"
            / "marketplace_summary"
        )

        (
            gold_df.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(str(gold_path))
        )

        # Gold JSON for presentation
        json_path = (
            BASE_DIR
            / "lake"
            / "gold"
            / "marketplace_summary_json"
        )

        (
            gold_df.coalesce(1)
            .write
            .mode("overwrite")
            .json(str(json_path))
        )

        print("\n========== GOLD SUMMARY ==========")

        gold_df.orderBy("source_system").show(
            truncate=False
        )

        print("\nGOLD PARQUET:", gold_path)
        print("GOLD JSON:", json_path)

        print("\nGOLD LAYER BUILD COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
