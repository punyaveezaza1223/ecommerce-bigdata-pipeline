"""Ingest public eBay listings into the external Raw Data Lake.

The filename is retained temporarily to minimize DAG and deployment changes.
It is now an eBay Browse API ingestion job, not a Shopee ingestion job.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "external" / "ebay"

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/{item_id}"
REQUEST_TIMEOUT_SECONDS = 30


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


def application_token(session: requests.Session) -> str:
    """Mint an eBay application token using client credentials."""
    response = session.post(
        TOKEN_URL,
        auth=(required_env("EBAY_CLIENT_ID"), required_env("EBAY_CLIENT_SECRET")),
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("eBay token response did not include access_token")
    return token


def get_item_details(
    session: requests.Session, headers: dict[str, str], item_id: str
) -> dict[str, Any]:
    """Fetch detail fields that are not always present in search results."""
    response = session.get(
        ITEM_URL.format(item_id=item_id),
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def money_value(value: Any) -> float | None:
    if not isinstance(value, dict):
        return None
    try:
        return float(value["value"])
    except (KeyError, TypeError, ValueError):
        return None


def category_value(item: dict[str, Any]) -> str | None:
    if item.get("categoryPath"):
        return item["categoryPath"]
    categories = item.get("categories") or []
    if categories:
        return categories[0].get("categoryName") or categories[0].get("categoryId")
    return None


def normalize_item(
    summary: dict[str, Any], details: dict[str, Any], collected_at: datetime
) -> dict[str, Any]:
    """Map eBay Browse responses to the existing common external schema."""
    item = {**summary, **details}
    price = money_value(item.get("price"))
    marketing_price = item.get("marketingPrice") or {}
    original_price = money_value(marketing_price.get("originalPrice"))
    if price is not None and original_price and original_price > 0:
        discount_percent = round(max((original_price - price) / original_price * 100, 0), 2)
    else:
        discount_percent = None

    review = item.get("primaryProductReviewRating") or {}
    seller = item.get("seller") or {}
    return {
        "platform": "ebay",
        "external_product_id": item.get("itemId"),
        "product_name": item.get("title"),
        "category": category_value(item),
        "brand": item.get("brand"),
        "currency": (item.get("price") or {}).get("currency"),
        "price": price,
        "original_price": original_price,
        "discount_percent": discount_percent,
        "rating": review.get("averageRating"),
        "review_count": review.get("reviewCount"),
        "sold_count": None,
        "seller_name": seller.get("username"),
        "product_url": item.get("itemWebUrl"),
        "collected_at": collected_at,
        "source_updated_at": item.get("itemOriginDate") or item.get("itemCreationDate"),
        "ingestion_timestamp": collected_at,
        "raw_source": "ebay_browse_api",
    }


def main() -> None:
    query = required_env("EBAY_SEARCH_QUERY")
    marketplace_id = required_env("EBAY_MARKETPLACE_ID")
    page_size = positive_int_env("EBAY_PAGE_SIZE", default=100, maximum=200)
    max_pages = positive_int_env("EXTERNAL_MAX_PAGES", default=1, maximum=100)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)

    with requests.Session() as session:
        token = application_token(session)
        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": marketplace_id,
        }
        records: list[dict[str, Any]] = []

        for page_number in range(max_pages):
            response = session.get(
                SEARCH_URL,
                headers=headers,
                params={"q": query, "limit": page_size, "offset": page_number * page_size},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            summaries = response.json().get("itemSummaries", [])
            if not summaries:
                break

            for summary in summaries:
                item_id = summary.get("itemId")
                details = get_item_details(session, headers, item_id) if item_id else {}
                record = normalize_item(summary, details, collected_at)
                if record["external_product_id"] and record["product_name"] and record["price"] is not None:
                    records.append(record)

            if len(summaries) < page_size:
                break

    frame = pd.DataFrame(records)
    if frame.empty:
        raise RuntimeError("eBay returned no valid records for the configured search")

    frame = frame.drop_duplicates(subset=["platform", "external_product_id"])
    output_file = OUTPUT_DIR / f"ebay_{collected_at:%Y-%m-%d}.parquet"
    frame.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    print(f"eBay records written: {len(frame):,}")
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    main()
