
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


DATA_ROOT = Path(os.environ.get("PIPELINE_DATA_ROOT", "/workspace/data"))
BASE = DATA_ROOT / "raw" / "external" / "etsy"
API_URL = "https://api.etsy.com/v3/application/listings/active"

KEYWORD = "handmade jewelry"
LIMIT = 20


def parse_price(value):
    if isinstance(value, dict):
        amount = value.get("amount")
        divisor = value.get("divisor")
        currency = value.get("currency_code")

        try:
            price = float(amount) / float(divisor)
            if float(divisor) == 0:
                return None, currency
            return price, currency
        except (ValueError, TypeError, ZeroDivisionError):
            return None, currency

    if isinstance(value, (int, float)):
        return float(value), None

    return None, None


def main():
    key = os.environ.get("ETSY_API_KEY")
    secret = os.environ.get("ETSY_SHARED_SECRET")

    if not key or not secret:
        raise RuntimeError(
            "Missing ETSY_API_KEY or ETSY_SHARED_SECRET"
        )

    params = urlencode({
        "keywords": KEYWORD,
        "limit": LIMIT,
        "offset": 0
    })

    request = Request(
        f"{API_URL}?{params}",
        headers={
            "x-api-key": f"{key}:{secret}",
            "Accept": "application/json"
        }
    )

    print("Connecting to Etsy API...")

    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)

            print(
                "Remaining daily API calls:",
                response.headers.get(
                    "x-remaining-today", "unknown"
                )
            )

    except HTTPError as error:
        print(f"Etsy API HTTP ERROR: {error.code}")
        raise SystemExit(1)

    except URLError as error:
        print(f"Etsy connection error: {error.reason}")
        raise SystemExit(1)

    listings = payload.get("results", [])

    if not listings:
        print("No listings returned. Nothing saved.")
        return

    print("Listings returned:", len(listings))

    BASE.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    # Save the original API response as JSON
    json_path = BASE / f"etsy_{timestamp}.json"

    with json_path.open("w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2
        )

    print("RAW JSON:", json_path)

    collected_at = datetime.now(
        timezone.utc
    ).isoformat()

    rows = []

    for listing in listings:
        price, currency = parse_price(
            listing.get("price")
        )

        listing_id = listing.get("listing_id")

        rows.append((
            "etsy",
            str(listing_id) if listing_id is not None else None,
            listing.get("title"),
            currency,
            price,
            listing.get("url"),
            collected_at,
            "etsy_api"
        ))

    schema = StructType([
        StructField("platform", StringType(), True),
        StructField(
            "external_product_id",
            StringType(),
            True
        ),
        StructField("product_name", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("product_url", StringType(), True),
        StructField("collected_at", StringType(), True),
        StructField("raw_source", StringType(), True)
    ])

    spark = (
        SparkSession.builder
        .appName("EtsyRawCollector")
        .master("local[1]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.createDataFrame(rows, schema=schema)

        parquet_path = BASE / f"etsy_{timestamp}.parquet"

        (
            df.coalesce(1)
            .write
            .mode("errorifexists")
            .parquet(str(parquet_path))
        )

        print("\nETSY RAW PARQUET CREATED")
        print("PATH:", parquet_path)
        print("ROWS:", df.count())

        df.select(
            "external_product_id",
            "product_name",
            "price",
            "currency"
        ).show(5, truncate=40)

        print("\nETSY COLLECTION SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
