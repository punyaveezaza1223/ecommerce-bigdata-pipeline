#!/usr/bin/env python3
"""Validate Advice Thailand raw snapshots and create canonical raw Parquet.

This adapter does not access the network or a database.  It reads the raw JSON
snapshots written by collect_advice_th.py and writes one non-overwriting Parquet
directory per snapshot for spark/spark_clean.py.
"""
import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ID_PATTERN = re.compile(r"^A[0-9]+$")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("PIPELINE_DATA_ROOT", "/workspace/data")),
        help="Data root (default: PIPELINE_DATA_ROOT or /workspace/data)",
    )
    return parser.parse_args()


def validate_snapshot(path: Path):
    with path.open("r", encoding="utf-8-sig") as stream:
        payload = json.load(stream)
    products = payload.get("products")
    if not isinstance(products, list) or not products:
        raise RuntimeError(f"{path}: missing non-empty products array")

    rows, seen = [], set()
    for product in products:
        product_id = str(product.get("external_product_id") or "").strip()
        name = str(product.get("product_name") or "").strip()
        currency = str(product.get("currency") or "").strip()
        observed = str(product.get("observed_at_utc") or "").strip()
        url = str(product.get("product_url") or "").strip()
        if product.get("source_system") != "advice_th":
            raise RuntimeError(f"{path}: unexpected source_system")
        if not ID_PATTERN.fullmatch(product_id) or product_id in seen:
            raise RuntimeError(f"{path}: invalid or duplicate product id {product_id!r}")
        if not name or currency != "THB" or not observed or not url:
            raise RuntimeError(f"{path}: incomplete required product fields")
        try:
            price = float(product["price"])
            datetime.fromisoformat(observed.replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"{path}: invalid price or observed timestamp") from exc
        if price <= 0:
            raise RuntimeError(f"{path}: non-positive price for {product_id}")
        seen.add(product_id)
        rows.append({
            "platform": "advice_th",
            "external_product_id": product_id,
            "product_name": name,
            "category": product.get("category"),
            "brand": None,
            "currency": "THB",
            "price": price,
            "original_price": None,
            "discount_percent": None,
            "rating": None,
            "review_count": None,
            "sold_count": None,
            "seller_name": None,
            "product_url": url,
            "image_url": None,
            "collected_at": observed,
            "source_updated_at": product.get("price_list_last_update"),
            "ingestion_timestamp": observed,
            "raw_source": "advice_th_public_price_list",
        })
    return rows


def main():
    args = parse_args()
    raw_dir = args.root / "raw" / "external" / "advice_th"
    output_dir = args.root / "raw" / "external" / "advice_th_parquet"
    inputs = sorted(raw_dir.glob("advice_th_*.json"))
    if not inputs:
        raise RuntimeError(f"No Advice raw JSON snapshots in {raw_dir}")

    snapshots = []
    for source in inputs:
        target = output_dir / source.stem
        if target.exists():
            print(f"SKIP_ALREADY_PREPARED: {source.name}", flush=True)
            continue
        snapshots.append((source, target, validate_snapshot(source)))
    if not snapshots:
        print("NO_NEW_ADVICE_SNAPSHOTS", flush=True)
        return

    spark = SparkSession.builder.appName("PrepareAdviceThailandProduction").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        for source, target, rows in snapshots:
            # Spark's error-if-exists mode protects a concurrent/manual rerun.
            frame = spark.createDataFrame(rows)
            valid = frame.filter(
                (F.col("platform") == "advice_th")
                & F.col("external_product_id").rlike(r"^A[0-9]+$")
                & (F.col("currency") == "THB")
                & (F.col("price") > 0)
            )
            if valid.count() != len(rows):
                raise RuntimeError(f"{source}: validation changed during Spark conversion")
            valid.write.mode("errorifexists").parquet(str(target))
            print(f"PREPARED: {source.name} rows={len(rows)} output={target}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
