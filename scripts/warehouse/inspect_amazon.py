#!/usr/bin/env python3
"""Profile Amazon historical metadata without inventing price observations.

Run with spark-submit. This only reads source Parquet and emits an aggregate
profile; it does not load a database or write to the warehouse.
"""
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ROOT = Path("/opt/spark/data")
SOURCE_ROOT = ROOT / "raw/historical/amazon_2023_parquet"
DATASETS = ("raw_meta_All_Beauty", "raw_meta_Amazon_Fashion", "raw_meta_Electronics")


def first_present(columns, candidates):
    return next((name for name in candidates if name in columns), None)


def main():
    spark = (SparkSession.builder.appName("InspectAmazonHistoricalMetadata")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        profile = {"source_kind": "historical Amazon Reviews 2023 metadata", "datasets": []}
        for name in DATASETS:
            path = SOURCE_ROOT / name
            files = list(path.glob("*.parquet")) if path.exists() else []
            if not files:
                continue
            # Read the directory rather than a glob. Spark 4 treats an absent
            # metadata path under a glob as a streaming-path warning/error.
            df = spark.read.parquet(str(path))
            asin = first_present(df.columns, ("parent_asin", "asin"))
            title = first_present(df.columns, ("title", "product_title"))
            price = first_present(df.columns, ("price", "priceUSD"))
            date = first_present(df.columns, ("collected_at", "observation_date", "price_date", "timestamp"))
            rating = first_present(df.columns, ("average_rating", "rating"))
            reviews = first_present(df.columns, ("rating_number", "review_count"))
            category = first_present(df.columns, ("main_category", "categories"))
            brand = first_present(df.columns, ("store", "brand"))
            row = {"dataset": name, "files": len(files), "columns": df.columns,
                   "id_column": asin, "product_name_column": title, "price_column": price,
                   "observation_date_column": date, "rating_column": rating,
                   "review_count_column": reviews, "category_column": category,
                   "brand_column": brand, "rows": df.count()}
            if asin:
                row["unique_product_ids"] = df.where(F.trim(F.col(asin)).isNotNull() &
                                                       (F.trim(F.col(asin)) != "")).select(asin).distinct().count()
            profile["datasets"].append(row)
        profile["has_verifiable_price_observation"] = any(
            item["price_column"] and item["observation_date_column"] for item in profile["datasets"])
        print(json.dumps(profile, indent=2, sort_keys=True), flush=True)
        if not profile["datasets"]:
            raise RuntimeError(f"No Amazon Parquet metadata found under {SOURCE_ROOT}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
