from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Amazon2023ProductAudit")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "/workspace/data/raw/historical/amazon_2023_parquet"

try:
    beauty = spark.read.parquet(
        base + "/raw_meta_All_Beauty/*.parquet"
    )

    electronics = spark.read.parquet(
        base + "/raw_meta_Electronics/*.parquet"
    )

    print("ALL_BEAUTY_ROWS:", beauty.count(), flush=True)
    print("ELECTRONICS_ROWS:", electronics.count(), flush=True)

    unique_products = (
        beauty.select("parent_asin")
        .unionByName(electronics.select("parent_asin"))
        .filter(F.col("parent_asin").isNotNull())
        .filter(F.trim(F.col("parent_asin")) != "")
        .dropDuplicates(["parent_asin"])
    )

    print(
        "UNIQUE_PRODUCTS_TOTAL:",
        unique_products.count(),
        flush=True
    )

finally:
    spark.stop()
