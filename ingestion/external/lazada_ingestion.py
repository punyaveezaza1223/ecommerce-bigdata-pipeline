"""Ingest public Mercado Libre listings into the external Raw Data Lake.

The filename is retained temporarily to minimize DAG and deployment changes.
It is now a Mercado Libre ingestion job, not a Lazada ingestion job.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "external" / "mercado_libre"

TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
SEARCH_URL = "https://api.mercadolibre.com/sites/{site_id}/search"
ITEMS_URL = "https://api.mercadolibre.com/items"
REQUEST_TIMEOUT_SECONDS = 30
MULTIGET_SIZE = 20


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


def access_token(session: requests.Session) -> str:
    """Use a supplied short-lived token or refresh one for the app owner account."""
    existing_token = os.getenv("MELI_ACCESS_TOKEN", "").strip()
    if existing_token:
        return existing_token

    response = session.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": required_env("MELI_CLIENT_ID"),
            "client_secret": required_env("MELI_CLIENT_SECRET"),
            "refresh_token": required_env("MELI_REFRESH_TOKEN"),
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("Mercado Libre token response did not include access_token")
    return token


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def get_items(
    session: requests.Session, headers: dict[str, str], item_ids: list[str]
) -> list[dict[str, Any]]:
    """Retrieve details through Mercado Libre's documented item multiget endpoint."""
    items: list[dict[str, Any]] = []
    for group in chunks(item_ids, MULTIGET_SIZE):
        response = session.get(
            ITEMS_URL,
            headers=headers,
            params={"ids": ",".join(group)},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        for entry in response.json():
            if entry.get("code") == 200 and isinstance(entry.get("body"), dict):
                items.append(entry["body"])
    return items


def attribute_value(item: dict[str, Any], attribute_id: str) -> str | None:
    for attribute in item.get("attributes") or []:
        if attribute.get("id") == attribute_id:
            return attribute.get("value_name")
    return None


def number_value(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def normalize_item(item: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    """Map Mercado Libre item details to the existing common external schema."""
    price = number_value(item.get("price"))
    original_price = number_value(item.get("original_price"))
    if price is not None and original_price and original_price > 0:
        discount_percent = round(max((original_price - price) / original_price * 100, 0), 2)
    else:
        discount_percent = None

    seller = item.get("seller") or {}
    seller_name = seller.get("nickname") or item.get("seller_id")
    return {
        "platform": "mercado_libre",
        "external_product_id": item.get("id"),
        "product_name": item.get("title"),
        "category": item.get("category_id"),
        "brand": attribute_value(item, "BRAND"),
        "currency": item.get("currency_id"),
        "price": price,
        "original_price": original_price,
        "discount_percent": discount_percent,
        "rating": None,
        "review_count": None,
        "sold_count": item.get("sold_quantity"),
        "seller_name": str(seller_name) if seller_name is not None else None,
        "product_url": item.get("permalink"),
        "collected_at": collected_at,
        "source_updated_at": item.get("last_updated"),
        "ingestion_timestamp": collected_at,
        "raw_source": "mercado_libre_items_api",
    }


def main() -> None:
    site_id = required_env("MELI_SITE_ID")
    query = required_env("MELI_SEARCH_QUERY")
    page_size = positive_int_env("MELI_PAGE_SIZE", default=50, maximum=50)
    max_pages = positive_int_env("EXTERNAL_MAX_PAGES", default=1, maximum=100)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)

    with requests.Session() as session:
        token = access_token(session)
        headers = {"Authorization": f"Bearer {token}"}
        records: list[dict[str, Any]] = []

        for page_number in range(max_pages):
            response = session.get(
                SEARCH_URL.format(site_id=site_id),
                headers=headers,
                params={"q": query, "limit": page_size, "offset": page_number * page_size},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            search_results = response.json().get("results", [])
            if not search_results:
                break

            item_ids = [item["id"] for item in search_results if item.get("id")]
            for item in get_items(session, headers, item_ids):
                record = normalize_item(item, collected_at)
                if record["external_product_id"] and record["product_name"] and record["price"] is not None:
                    records.append(record)

            if len(search_results) < page_size:
                break

    frame = pd.DataFrame(records)
    if frame.empty:
        raise RuntimeError("Mercado Libre returned no valid records for the configured search")

    frame = frame.drop_duplicates(subset=["platform", "external_product_id"])
    output_file = OUTPUT_DIR / f"mercado_libre_{collected_at:%Y-%m-%d}.parquet"
    frame.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    print(f"Mercado Libre records written: {len(frame):,}")
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    main()
