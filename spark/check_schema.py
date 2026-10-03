from pyspark.sql import SparkSession


INPUT_PATH = "/opt/spark/data/processed/products/external_products"


spark = (
    SparkSession.builder
    .appName("Check Processed External Product Schema")
    .getOrCreate()
)

try:
    df = spark.read.parquet(INPUT_PATH)

    print("\n========== PROCESSED SCHEMA ==========")
    df.printSchema()

    print("\n========== COLUMNS ==========")
    for column in df.columns:
        print(column)

    print(f"\n========== ROW COUNT: {df.count():,} ==========")
finally:
    spark.stop()
