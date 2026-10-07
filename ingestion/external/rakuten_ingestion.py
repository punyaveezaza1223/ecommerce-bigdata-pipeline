"""Ingest Rakuten Ichiba listings into the external Raw Data Lake."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "external" / "rakuten"

SEARCH_URL = "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701"
REQUEST_TIMEOUT_SECONDS = 30

MAX_HITS_PER_PAGE = 30
MAX_PAGES = 100
DEFAULT_EXTERNAL_MAX_PAGES = 1

# Protect against Rakuten API rate limits.
REQUEST_DELAY_SECONDS = 2
MAX_RETRIES = 5
DEFAULT_RETRY_WAIT_SECONDS = 5
MAX_RETRY_WAIT_SECONDS = 60


def required_env(name: str) -> str:
    """Return a required environment variable without printing its value."""
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def search_queries() -> list[str]:
    """Read multiple Rakuten queries, with backward compatibility for one query."""
    raw_queries = os.getenv("RAKUTEN_SEARCH_QUERIES", "").strip()

    if raw_queries:
        queries = [
            query.strip()
            for query in raw_queries.split(",")
            if query.strip()
        ]
    else:
        queries = [required_env("RAKUTEN_SEARCH_QUERY")]

    if not queries:
        raise RuntimeError("No Rakuten search queries configured")

    return queries


def positive_int_env(
    name: str,
    maximum: int,
    default: int | None = None,
) -> int:
    raw_value = os.getenv(name, "").strip()

    if not raw_value:
        if default is None:
            raise RuntimeError(
                f"Missing required environment variable: {name}"
            )

        raw_value = str(default)

    try:
        value = int(raw_value)
    except ValueError as error:
        raise RuntimeError(
            f"{name} must be an integer"
        ) from error

    if not 1 <= value <= maximum:
        raise RuntimeError(
            f"{name} must be between 1 and {maximum}"
        )

    return value


def number_value(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def first_image_url(image_urls: Any) -> str | None:
    """Return the first usable image URL from a Rakuten image URL list."""
    if not isinstance(image_urls, list) or not image_urls:
        return None

    first_image = image_urls[0]

    if isinstance(first_image, str):
        return first_image.strip() or None

    if isinstance(first_image, dict):
        for key in ("imageUrl", "url", "image_url"):
            value = first_image.get(key)

            if isinstance(value, str) and value.strip():
                return value.strip()

    return None


def request_item_search(
    session: requests.Session,
    params: dict[str, Any],
    access_key: str,
) -> dict[str, Any]:
    """
    Call Rakuten safely.

    HTTP 429 is retried with Retry-After when available,
    otherwise exponential backoff is used.

    Request parameters and credentials are not included
    in error messages.
    """

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = session.get(
                SEARCH_URL,
                params=params,
                headers={"accessKey": access_key},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

        except requests.Timeout:
            raise RuntimeError(
                "Rakuten API request failed: timeout"
            ) from None

        except requests.RequestException:
            raise RuntimeError(
                "Rakuten API request failed: network error"
            ) from None

        if response.status_code == 429:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(
                    "Rakuten API request failed: "
                    "HTTP 429 after retries"
                ) from None

            retry_after = response.headers.get("Retry-After")

            try:
                if retry_after:
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = (
                        DEFAULT_RETRY_WAIT_SECONDS
                        * (2 ** attempt)
                    )
            except (TypeError, ValueError):
                wait_seconds = (
                    DEFAULT_RETRY_WAIT_SECONDS
                    * (2 ** attempt)
                )

            wait_seconds = max(
                1,
                min(
                    wait_seconds,
                    MAX_RETRY_WAIT_SECONDS,
                ),
            )

            print(
                "Rakuten rate limit reached (HTTP 429). "
                f"Retrying in {wait_seconds:g} seconds "
                f"({attempt + 1}/{MAX_RETRIES})..."
            )

            time.sleep(wait_seconds)
            continue

        if not response.ok:
            raise RuntimeError(
                f"Rakuten API request failed: "
                f"HTTP {response.status_code}"
            ) from None

        try:
            response_data = response.json()
        except ValueError:
            raise RuntimeError(
                "Rakuten API request failed: "
                "invalid JSON response"
            ) from None

        if not isinstance(response_data, dict):
            raise RuntimeError(
                "Rakuten returned an unexpected response structure"
            )

        return response_data

    raise RuntimeError(
        "Rakuten API request failed after retries"
    )


def normalize_item(
    item: dict[str, Any],
    collected_at: datetime,
) -> dict[str, Any]:
    """Map a Rakuten Item Search result to the common external schema."""
    return {
        "platform": "rakuten",
        "external_product_id": item.get("itemCode"),
        "product_name": item.get("itemName"),
        "category": (
            str(item["genreId"])
            if item.get("genreId") is not None
            else None
        ),
        "brand": None,
        "currency": "JPY",
        "price": number_value(item.get("itemPrice")),
        "original_price": None,
        "discount_percent": None,
        "rating": number_value(item.get("reviewAverage")),
        "review_count": item.get("reviewCount"),
        "sold_count": None,
        "seller_name": item.get("shopName"),
        "product_url": (
            item.get("affiliateUrl")
            or item.get("itemUrl")
        ),
        "image_url": (
            first_image_url(item.get("mediumImageUrls"))
            or first_image_url(item.get("smallImageUrls"))
        ),
        "collected_at": collected_at,
        "source_updated_at": None,
        "ingestion_timestamp": collected_at,
        "raw_source": "rakuten_ichiba_item_search_api",
    }


def main() -> None:
    application_id = required_env("RAKUTEN_APPLICATION_ID")
    access_key = required_env("RAKUTEN_ACCESS_KEY")

    queries = search_queries()

    page_size = positive_int_env(
        "RAKUTEN_PAGE_SIZE",
        MAX_HITS_PER_PAGE,
    )

    max_pages = positive_int_env(
        "EXTERNAL_MAX_PAGES",
        MAX_PAGES,
        DEFAULT_EXTERNAL_MAX_PAGES,
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    collected_at = datetime.now(
        timezone.utc
    ).replace(microsecond=0)

    records: list[dict[str, Any]] = []

    with requests.Session() as session:
        request_count = 0

        for query in queries:
            query_count = 0

            print(f"Searching Rakuten query: {query}")

            for page in range(1, max_pages + 1):
                # Avoid sending normal API requests back-to-back.
                if request_count > 0:
                    time.sleep(REQUEST_DELAY_SECONDS)

                params = {
                    "applicationId": application_id,
                    "keyword": query,
                    "format": "json",
                    "formatVersion": 2,
                    "hits": page_size,
                    "page": page,
                    "elements": (
                        "itemCode,itemName,itemPrice,itemUrl,"
                        "affiliateUrl,smallImageUrls,"
                        "mediumImageUrls,genreId,shopName,"
                        "reviewAverage,reviewCount"
                    ),
                }

                response_data = request_item_search(
                    session,
                    params,
                    access_key,
                )

                request_count += 1

                # Support both response key variants.
                items = (
                    response_data.get("items")
                    or response_data.get("Items")
                    or []
                )

                if not isinstance(items, list):
                    raise RuntimeError(
                        "Rakuten returned an unexpected item list"
                    )

                if not items:
                    break

                for item in items:
                    record = normalize_item(
                        item,
                        collected_at,
                    )

                    if (
                        record["external_product_id"]
                        and record["product_name"]
                        and record["price"] is not None
                    ):
                        records.append(record)
                        query_count += 1

                page_count = response_data.get("pageCount")

                if len(items) < page_size:
                    break

                if (
                    page_count is not None
                    and page >= int(page_count)
                ):
                    break

            print(
                f"Rakuten query '{query}' valid records: "
                f"{query_count:,}"
            )

    frame = pd.DataFrame(records)

    if frame.empty:
        raise RuntimeError(
            "Rakuten returned no valid records "
            "for the configured searches"
        )

    # A product may appear in multiple search queries.
    frame = frame.drop_duplicates(
        subset=[
            "platform",
            "external_product_id",
        ]
    )

    output_file = (
        OUTPUT_DIR
        / f"rakuten_{collected_at:%Y-%m-%d}.parquet"
    )

    frame.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )

    print(
        f"Rakuten unique records written: "
        f"{len(frame):,}"
    )
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    main()