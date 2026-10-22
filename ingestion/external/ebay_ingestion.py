"""Ingest public eBay listings into the external Raw Data Lake."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "external" / "ebay"

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/{item_id}"
REQUEST_TIMEOUT_SECONDS = 30
MAX_PAGE_SIZE = 200
MAX_PAGES = 50

# Keep every eBay Raw batch compatible with the historical Parquet schema.
# In particular, Search summaries can omit rating/review fields entirely;
# Pandas would otherwise infer a Parquet null (physical INT32) column.
RAW_SCHEMA = pa.schema(
    [
        pa.field("platform", pa.string()),
        pa.field("external_product_id", pa.string()),
        pa.field("product_name", pa.string()),
        pa.field("category", pa.string()),
        pa.field("brand", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("original_price", pa.float64()),
        pa.field("discount_percent", pa.float64()),
        pa.field("rating", pa.string()),
        pa.field("review_count", pa.float64()),
        pa.field("sold_count", pa.null()),
        pa.field("seller_name", pa.string()),
        pa.field("product_url", pa.string()),
        pa.field("image_url", pa.string()),
        pa.field("collected_at", pa.timestamp("ms", tz="UTC")),
        pa.field("source_updated_at", pa.string()),
        pa.field("ingestion_timestamp", pa.timestamp("ms", tz="UTC")),
        pa.field("raw_source", pa.string()),
    ]
)


def required_env(name: str) -> str:
    """Return a required environment variable without printing its value."""
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value


def search_queries() -> list[str]:
    """Read multiple eBay queries, with backward compatibility for one query."""
    raw_queries = os.getenv(
        "EBAY_SEARCH_QUERIES",
        "",
    ).strip()

    if raw_queries:
        queries = [
            query.strip()
            for query in raw_queries.split(",")
            if query.strip()
        ]
    else:
        queries = [
            required_env("EBAY_SEARCH_QUERY")
        ]

    if not queries:
        raise RuntimeError(
            "No eBay search queries configured"
        )

    return queries


def positive_int_env(
    name: str,
    default: int,
    maximum: int,
) -> int:
    try:
        value = int(
            os.getenv(name, str(default))
        )
    except ValueError as error:
        raise RuntimeError(
            f"{name} must be an integer"
        ) from error

    if not 1 <= value <= maximum:
        raise RuntimeError(
            f"{name} must be between 1 and {maximum}"
        )

    return value


def boolean_env(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable with an explicit error on typos."""
    raw_value = os.getenv(name, str(default)).strip().lower()

    if raw_value in {"1", "true", "yes", "on"}:
        return True

    if raw_value in {"0", "false", "no", "off"}:
        return False

    raise RuntimeError(
        f"{name} must be a boolean (true or false)"
    )


def ebay_max_pages() -> int:
    """Prefer eBay's limit while retaining the legacy shared setting."""
    if os.getenv("EBAY_MAX_PAGES", "").strip():
        return positive_int_env(
            "EBAY_MAX_PAGES",
            default=1,
            maximum=MAX_PAGES,
        )

    return positive_int_env(
        "EXTERNAL_MAX_PAGES",
        default=1,
        maximum=MAX_PAGES,
    )


def application_token(
    session: requests.Session,
) -> str:
    """Mint an eBay application token using client credentials."""
    response = session.post(
        TOKEN_URL,
        auth=(
            required_env("EBAY_CLIENT_ID"),
            required_env("EBAY_CLIENT_SECRET"),
        ),
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        headers={
            "Content-Type":
                "application/x-www-form-urlencoded",
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    token = response.json().get(
        "access_token"
    )

    if not token:
        raise RuntimeError(
            "eBay token response did not include access_token"
        )

    return token


def get_item_details(
    session: requests.Session,
    headers: dict[str, str],
    item_id: str,
) -> dict[str, Any]:
    """
    Fetch detail fields that are not always present in search results.

    Some items returned by Browse Search may no longer be available
    from the Item Detail endpoint. HTTP 404 therefore falls back to
    the search summary instead of failing the entire ingestion.
    """
    try:
        response = session.get(
            ITEM_URL.format(
                item_id=item_id
            ),
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.Timeout:
        raise RuntimeError(
            "eBay item detail request failed: timeout"
        ) from None
    except requests.RequestException:
        raise RuntimeError(
            "eBay item detail request failed: network error"
        ) from None

    if response.status_code == 404:
        print(
            "eBay item detail unavailable (HTTP 404); "
            "using search summary instead."
        )
        return {}

    if not response.ok:
        raise RuntimeError(
            "eBay item detail request failed: "
            f"HTTP {response.status_code}"
        ) from None

    try:
        data = response.json()
    except ValueError:
        raise RuntimeError(
            "eBay item detail returned invalid JSON"
        ) from None

    if not isinstance(data, dict):
        return {}

    return data


def money_value(
    value: Any,
) -> float | None:
    if not isinstance(value, dict):
        return None

    try:
        return float(
            value["value"]
        )
    except (
        KeyError,
        TypeError,
        ValueError,
    ):
        return None


def number_value(value: Any) -> float | None:
    """Convert an API scalar to a numeric value for the Raw schema."""
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def string_value(value: Any) -> str | None:
    """Return a non-empty string, preserving the historical Raw type."""
    if value is None:
        return None

    value = str(value).strip()
    return value or None


def category_value(
    item: dict[str, Any],
) -> str | None:
    if item.get("categoryPath"):
        return item["categoryPath"]

    categories = (
        item.get("categories")
        or []
    )

    if categories:
        return (
            categories[0].get(
                "categoryName"
            )
            or categories[0].get(
                "categoryId"
            )
        )

    return None


def image_url_value(item: dict[str, Any]) -> str | None:
    """Return the primary image URL supplied by Browse Search or Item."""
    image = item.get("image") or {}

    if not isinstance(image, dict):
        return None

    for key in ("imageUrl", "url"):
        value = image.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


def normalize_item(
    summary: dict[str, Any],
    details: dict[str, Any],
    collected_at: datetime,
) -> dict[str, Any]:
    """Map eBay Browse responses to the common external schema."""
    item = {
        **summary,
        **details,
    }

    price = money_value(
        item.get("price")
    )

    marketing_price = (
        item.get("marketingPrice")
        or {}
    )

    original_price = money_value(
        marketing_price.get(
            "originalPrice"
        )
    )

    if (
        price is not None
        and original_price
        and original_price > 0
    ):
        discount_percent = round(
            max(
                (
                    original_price
                    - price
                )
                / original_price
                * 100,
                0,
            ),
            2,
        )
    else:
        discount_percent = None

    review = (
        item.get(
            "primaryProductReviewRating"
        )
        or {}
    )

    seller = (
        item.get("seller")
        or {}
    )

    return {
        "platform": "ebay",
        "external_product_id":
            item.get("itemId"),
        "product_name":
            item.get("title"),
        "category":
            category_value(item),
        "brand":
            string_value(item.get("brand")),
        "currency":
            (
                item.get("price")
                or {}
            ).get("currency"),
        "price":
            price,
        "original_price":
            original_price,
        "discount_percent":
            discount_percent,
        "rating":
            string_value(review.get("averageRating")),
        "review_count":
            number_value(review.get("reviewCount")),
        "sold_count":
            None,
        "seller_name":
            seller.get(
                "username"
            ),
        "product_url":
            item.get(
                "itemWebUrl"
            ),
        "image_url": image_url_value(item),
        "collected_at":
            collected_at,
        "source_updated_at":
            (
                item.get(
                    "itemOriginDate"
                )
                or item.get(
                    "itemCreationDate"
                )
            ),
        "ingestion_timestamp":
            collected_at,
        "raw_source":
            "ebay_browse_api",
    }


def main() -> None:
    queries = search_queries()

    marketplace_id = required_env(
        "EBAY_MARKETPLACE_ID"
    )

    page_size = positive_int_env(
        "EBAY_PAGE_SIZE",
        default=100,
        maximum=MAX_PAGE_SIZE,
    )

    # EBAY_MAX_PAGES lets eBay use a broader search window without
    # increasing the page count used by Rakuten and other integrations.
    # EXTERNAL_MAX_PAGES remains a backward-compatible fallback.
    max_pages = ebay_max_pages()

    # Browse Search already supplies every field required by our Raw schema.
    # Item Detail costs one extra Browse call per unique listing, so keep it
    # opt-in for rare enrichment/backfill runs.
    fetch_item_details = boolean_env(
        "EBAY_FETCH_ITEM_DETAILS",
        default=False,
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    collected_at = datetime.now(
        timezone.utc
    ).replace(
        microsecond=0
    )

    records: list[
        dict[str, Any]
    ] = []
    seen_item_ids: set[str] = set()

    with requests.Session() as session:
        token = application_token(
            session
        )

        headers = {
            "Authorization":
                f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID":
                marketplace_id,
        }

        if fetch_item_details:
            print(
                "eBay Item Detail enrichment is enabled; "
                "this adds one API call for each unique listing."
            )
        else:
            print(
                "Using eBay Search summaries only; "
                "Item Detail enrichment is disabled."
            )

        for query in queries:
            query_count = 0

            print(
                f"Searching eBay query: {query}"
            )

            for page_number in range(
                max_pages
            ):
                response = session.get(
                    SEARCH_URL,
                    headers=headers,
                    params={
                        "q": query,
                        "limit":
                            page_size,
                        "offset":
                            (
                                page_number
                                * page_size
                            ),
                    },
                    timeout=
                        REQUEST_TIMEOUT_SECONDS,
                )

                response.raise_for_status()

                summaries = (
                    response.json().get(
                        "itemSummaries",
                        [],
                    )
                )

                if not summaries:
                    break

                for summary in summaries:
                    item_id = (
                        summary.get(
                            "itemId"
                        )
                    )

                    # Search terms overlap.  Deduplicate before any optional
                    # Detail request so the same listing never consumes quota
                    # twice in a single ingestion run.
                    if not item_id or item_id in seen_item_ids:
                        continue

                    seen_item_ids.add(item_id)

                    details = (
                        get_item_details(
                            session,
                            headers,
                            item_id,
                        )
                        if fetch_item_details
                        else {}
                    )

                    record = (
                        normalize_item(
                            summary,
                            details,
                            collected_at,
                        )
                    )

                    if (
                        record[
                            "external_product_id"
                        ]
                        and record[
                            "product_name"
                        ]
                        and record[
                            "price"
                        ]
                        is not None
                    ):
                        records.append(
                            record
                        )
                        query_count += 1

                if (
                    len(summaries)
                    < page_size
                ):
                    break

            print(
                f"eBay query '{query}' "
                f"valid records: "
                f"{query_count:,}"
            )

    frame = pd.DataFrame(
        records
    )

    if frame.empty:
        raise RuntimeError(
            "eBay returned no valid records "
            "for the configured searches"
        )

    # The same product may appear
    # in more than one search query.
    frame = frame.drop_duplicates(
        subset=[
            "platform",
            "external_product_id",
        ]
    )

    # Include UTC collection time in the filename so multiple
    # ingestion runs on the same day do not overwrite each other.
    output_file = (
        OUTPUT_DIR
        / (
            f"ebay_"
            f"{collected_at:%Y-%m-%d_%H%M%S}"
            f".parquet"
        )
    )

    table = pa.Table.from_pandas(
        frame,
        schema=RAW_SCHEMA,
        preserve_index=False,
    )

    pq.write_table(
        table,
        output_file,
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )

    print(
        "eBay unique records written: "
        f"{len(frame):,}"
    )

    print(
        f"Saved: {output_file}"
    )


if __name__ == "__main__":
    main()
