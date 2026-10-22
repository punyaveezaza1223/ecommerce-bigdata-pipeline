
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path("/workspace")
OUTPUT = ROOT / "data/processed_test/hybrid_csv_warehouse_input_v2"

SOURCES = [
    {
        "platform": "amazon_csv",
        "path": ROOT / "temp_lazada/amazon-products.csv",
        "id": "asin",
        "name": "title",
        "price": "final_price",
        "rating": "rating",
        "reviews": "reviews_count",
        "seller": "seller_name",
        "category": "categories",
        "brand": "brand",
        "url": "url",
        "date": "timestamp",
    },
    {
        "platform": "walmart_csv",
        "path": ROOT / "temp_lazada/walmart-products.csv",
        "id": "product_id",
        "name": "product_name",
        "price": "final_price",
        "rating": "rating",
        "reviews": "review_count",
        "seller": "seller",
        "category": "category_name",
        "brand": "brand",
        "url": "url",
        "date": "timestamp",
    },
]


def main():
    if OUTPUT.exists():
        raise RuntimeError(
            f"Output already exists: {OUTPUT}. "
            "Stopping to protect previous results."
        )

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("HybridAmazonWalmartCSV")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        frames = []

        for src in SOURCES:
            if not src["path"].is_file():
                raise RuntimeError(
                    f"CSV not found: {src['path']}"
                )

            print(f"\nREADING: {src['platform']}", flush=True)

            raw = (
                spark.read
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
                .option("encoding", "UTF-8")
                .option("mode", "FAILFAST")
                .csv(str(src["path"]))
            )

            required = [
                src["id"],
                src["name"],
                src["date"],
            ]

            missing = [
                name for name in required
                if name not in raw.columns
            ]

            if missing:
                raise RuntimeError(
                    f"{src['platform']} missing columns: {missing}"
                )

            def field(name):
                column_name = src.get(name)
                if column_name and column_name in raw.columns:
                    value = F.trim(F.col(column_name).cast("string"))
                    return F.when(
                        value.isin("", "null", "None", "N/A"),
                        F.lit(None)
                    ).otherwise(value)
                return F.lit(None).cast("string")

            price = field("price").cast("double")
            rating = field("rating").cast("double")
            reviews = field("reviews").cast("long")

            collected_at = F.to_timestamp(
                field("date"),
                "yyyy-MM-dd HH:mm:ss.SSS"
            )

            df = raw.select(
                F.lit(src["platform"]).alias("platform"),
                field("id").alias("external_product_id"),
                field("name").alias("product_name"),
                field("category").alias("category"),
                field("brand").alias("brand"),
                F.lit("USD").alias("currency"),
                F.when(price >= 0, price).alias("price"),
                F.lit(None).cast("double").alias("original_price"),
                F.lit(None).cast("double").alias("discount_percent"),
                F.when(
                    (rating >= 0) & (rating <= 5),
                    rating
                ).alias("rating"),
                F.when(reviews >= 0, reviews).alias("review_count"),
                F.lit(None).cast("long").alias("sold_count"),
                field("seller").alias("seller_name"),
                field("url").alias("product_url"),
                F.lit(None).cast("string").alias("image_url"),
                collected_at.alias("collected_at"),
                F.lit(None).cast("timestamp").alias("source_updated_at"),
                F.lit(None).cast("timestamp").alias("ingestion_timestamp"),
                F.lit("historical_csv").alias("raw_source"),
                F.to_date(collected_at).alias("observation_date"),
                F.current_timestamp().alias("processed_at"),
            )

            df = df.filter(
                F.col("external_product_id").isNotNull()
                & (F.col("external_product_id") != "")
                & F.col("product_name").isNotNull()
                & (F.col("product_name") != "")
                & F.col("observation_date").isNotNull()
            )

            # One product per source in this historical snapshot.
            df = df.dropDuplicates(
                ["platform", "external_product_id"]
            )

            print(
                f"{src['platform']} unique valid products: {df.count()}",
                flush=True
            )

            frames.append(df)

        combined = frames[0].unionByName(frames[1])

        total = combined.count()

        print("\nHYBRID CSV SUMMARY", flush=True)
        combined.groupBy("platform").count().show(
            truncate=False
        )

        print("TOTAL UNIQUE SOURCE PRODUCTS:", total)

        if total == 0:
            raise RuntimeError("No valid products found")

        print("\nOUTPUT SCHEMA")
        combined.printSchema()

        (
            combined.coalesce(2)
            .write
            .mode("errorifexists")
            .parquet(str(OUTPUT))
        )

        print("\nHYBRID CSV IMPORT SUCCESS")
        print("OUTPUT:", OUTPUT)
        print("ROWS:", total)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
