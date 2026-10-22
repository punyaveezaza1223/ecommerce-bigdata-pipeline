import csv
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path("/workspace")
SOURCE = ROOT / "data/processed_test/advice_th_warehouse_input_v1"
DEST = ROOT / "data/processed_test/advice_th_dim_import_v1.csv"

if not (SOURCE / "_SUCCESS").is_file():
    raise RuntimeError("Spark output is not complete")

if DEST.exists():
    raise RuntimeError("CSV already exists; refusing to overwrite")

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("AdviceDimExport")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(str(SOURCE))
    rows = df.select(
        "source_system",
        "external_product_id",
        "product_name",
        "category",
        "currency"
    ).collect()

    if len(rows) != 196:
        raise RuntimeError(f"Expected 196 rows, got {len(rows)}")

    records = []
    seen = set()

    for row in rows:
        source, pid, name, category, currency = row

        if (
            source != "advice_th"
            or currency != "THB"
            or not pid
            or not pid.startswith("A")
            or not name
            or (source, pid) in seen
        ):
            raise RuntimeError(f"Invalid or duplicate product: {pid}")

        seen.add((source, pid))
        records.append((source, pid, name, category or ""))

    with DEST.open("x", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_system", "source_product_id",
            "product_name", "category"
        ])
        writer.writerows(records)

    print("CSV_EXPORT_SUCCESS:", DEST, flush=True)
    print("ROWS:", len(records), flush=True)

finally:
    spark.stop()
