"""Ingest Best Buy product data into the external Raw Data Lake.

The filename is retained temporarily to minimize DAG and deployment changes.
Best Buy API content is retained in this Raw directory for at most 72 hours.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "external" / "best_buy"

PRODUCTS_URL = "https://api.bestbuy.com/v1/products({query})"
REQUEST_TIMEOUT_SECONDS = 30
RETENTION_HOURS = 72


def required_env(name: str) -> str:
    """Return a required environment variable without printing its value."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def positive_int_env(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if not 1 <= value <= maximum:
        raise RuntimeError(f"{name} must be between 1 and {maximum}")
    return value


def number_value(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def category_value(product: dict[str, Any]) -> str | None:
    category_path = product.get("categoryPath") or []
    if not category_path:
        return None
    categories = [category.get("name") for category in category_path if category.get("name")]
    return " > ".join(categories) if categories else None


def normalize_product(
    product: dict[str, Any], collected_at: datetime, currency: str
) -> dict[str, Any]:
    """Map a Best Buy Products response to the common external product schema."""
    price = number_value(product.get("salePrice"))
    original_price = number_value(product.get("regularPrice"))
    if price is not None and original_price and original_price > 0:
        discount_percent = round(max((original_price - price) / original_price * 100, 0), 2)
    else:
        discount_percent = None

    return {
        "platform": "best_buy",
        "external_product_id": str(product["sku"]) if product.get("sku") is not None else None,
        "product_name": product.get("name"),
        "category": category_value(product),
        "brand": product.get("manufacturer"),
        "currency": currency,
        "price": price,
        "original_price": original_price,
        "discount_percent": discount_percent,
        "rating": number_value(product.get("customerReviewAverage")),
        "review_count": product.get("customerReviewCount"),
        "sold_count": None,
        "seller_name": "Best Buy",
        "product_url": product.get("url"),
        "collected_at": collected_at,
        "source_updated_at": product.get("dateUpdate"),
        "ingestion_timestamp": collected_at,
        "raw_source": "best_buy_products_api",
    }


def purge_expired_raw_files(now: datetime) -> int:
    """Remove only Best Buy Raw Parquet files older than the contractual retention."""
    cutoff = now - timedelta(hours=RETENTION_HOURS)
    removed = 0
    for file_path in OUTPUT_DIR.glob("best_buy_*.parquet"):
        modified_at = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        if modified_at < cutoff:
            file_path.unlink()
            removed += 1
    return removed


def main() -> None:
    query = required_env("BESTBUY_QUERY")
    api_key = required_env("BESTBUY_API_KEY")
    currency = required_env("BESTBUY_CURRENCY")
    page_size = positive_int_env("BESTBUY_PAGE_SIZE", default=100, maximum=100)
    max_pages = positive_int_env("EXTERNAL_MAX_PAGES", default=1, maximum=100)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)
    removed = purge_expired_raw_files(collected_at)
    if removed:
        print(f"Expired Best Buy Raw files removed: {removed}")

    records: list[dict[str, Any]] = []
    with requests.Session() as session:
        for page_number in range(1, max_pages + 1):
            response = session.get(
                PRODUCTS_URL.format(query=query),
                params={
                    "apiKey": api_key,
                    "format": "json",
                    "page": page_number,
                    "pageSize": page_size,
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            products = response.json().get("products", [])
            if not products:
                break

            for product in products:
                record = normalize_product(product, collected_at, currency)
                if record["external_product_id"] and record["product_name"] and record["price"] is not None:
                    records.append(record)

            if len(products) < page_size:
                break

    frame = pd.DataFrame(records)
    if frame.empty:
        raise RuntimeError("Best Buy returned no valid records for the configured query")

    frame = frame.drop_duplicates(subset=["platform", "external_product_id"])
    output_file = OUTPUT_DIR / f"best_buy_{collected_at:%Y-%m-%d}.parquet"
    frame.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    print(f"Best Buy records written: {len(frame):,}")
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    main()
