"""Ingest StockX Catalog Search products into the external Raw Data Lake."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "external" / "stockx"
TOKEN_FILE_NAME = ".stockx_oauth_tokens.json"
TOKEN_URL = "https://accounts.stockx.com/oauth/token"
CATALOG_SEARCH_URL = "https://api.stockx.com/v2/catalog/search"
REQUEST_TIMEOUT_SECONDS = 30
MAX_PAGE_SIZE = 50
MAX_PAGES = 100
MIN_REQUEST_DELAY_SECONDS = 1.0

# Catalog Search is product metadata. It does not provide a current market
# price, currency, image URL, seller, or review metrics, so those fields are
# intentionally stored as null rather than inferred.
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
        pa.field("rating", pa.float64()),
        pa.field("review_count", pa.float64()),
        pa.field("sold_count", pa.float64()),
        pa.field("seller_name", pa.string()),
        pa.field("product_url", pa.string()),
        pa.field("image_url", pa.string()),
        pa.field("collected_at", pa.timestamp("ms", tz="UTC")),
        pa.field("source_updated_at", pa.string()),
        pa.field("ingestion_timestamp", pa.timestamp("ms", tz="UTC")),
        pa.field("raw_source", pa.string()),
    ]
)


def load_local_env() -> None:
    """Load non-exported local environment values without logging them."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.is_file():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        name, value = line.split("=", 1)
        name = name.strip()
        if name and name not in os.environ:
            os.environ[name] = value.strip().strip("\"'")


def required_env(name: str) -> str:
    """Read a credential without including its value in messages."""
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


def request_delay_seconds() -> float:
    try:
        configured = float(os.getenv("STOCKX_REQUEST_DELAY_SECONDS", "1"))
    except ValueError as error:
        raise RuntimeError("STOCKX_REQUEST_DELAY_SECONDS must be a number") from error
    if configured < MIN_REQUEST_DELAY_SECONDS:
        raise RuntimeError("STOCKX_REQUEST_DELAY_SECONDS must be at least 1 second")
    if configured > 60:
        raise RuntimeError("STOCKX_REQUEST_DELAY_SECONDS must not exceed 60 seconds")
    return configured


def search_queries() -> list[str]:
    raw_queries = os.getenv("STOCKX_SEARCH_QUERIES", "").strip()
    if not raw_queries:
        raise RuntimeError("Missing required environment variable: STOCKX_SEARCH_QUERIES")
    queries = [query.strip() for query in raw_queries.split(",") if query.strip()]
    if not queries:
        raise RuntimeError("No StockX search queries configured")
    return queries


def token_file_path() -> Path:
    configured = os.getenv("STOCKX_TOKEN_FILE", "").strip()
    if not configured:
        return PROJECT_ROOT / TOKEN_FILE_NAME
    path = Path(configured)
    return path if path.is_absolute() else PROJECT_ROOT / path


def read_token_file() -> dict[str, Any]:
    path = token_file_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise RuntimeError("Unable to read the local StockX token file") from error
    return data if isinstance(data, dict) else {}


def store_tokens(token_response: dict[str, Any]) -> None:
    access_token = token_response.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError("StockX token refresh did not include access_token")

    previous_tokens = read_token_file()
    refresh_token = token_response.get("refresh_token")
    token_data = {
        "access_token": access_token,
        "refresh_token": (
            refresh_token
            if isinstance(refresh_token, str) and refresh_token
            else previous_tokens.get("refresh_token")
        ),
        "expires_in": token_response.get("expires_in"),
        "received_at": int(time.time()),
    }
    path = token_file_path()
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(token_data), encoding="utf-8")
    os.chmod(temporary_path, 0o600)
    temporary_path.replace(path)


def access_token() -> str:
    configured = os.getenv("STOCKX_ACCESS_TOKEN", "").strip()
    if configured:
        return configured
    token = read_token_file().get("access_token", "")
    if isinstance(token, str) and token:
        return token
    raise RuntimeError("No StockX access token is available")


def refresh_access_token() -> str:
    refresh_token = (
        os.getenv("STOCKX_REFRESH_TOKEN", "").strip()
        or read_token_file().get("refresh_token", "")
    )
    if not isinstance(refresh_token, str) or not refresh_token:
        raise RuntimeError("No StockX refresh token is available")

    try:
        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": required_env("STOCKX_CLIENT_ID"),
                "client_secret": required_env("STOCKX_CLIENT_SECRET"),
                "audience": "gateway.stockx.com",
                "refresh_token": refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        raise RuntimeError("StockX token refresh failed: network error") from None

    if not response.ok:
        raise RuntimeError(f"StockX token refresh failed: HTTP {response.status_code}")
    try:
        token_response = response.json()
    except ValueError as error:
        raise RuntimeError("StockX token refresh returned invalid JSON") from error
    if not isinstance(token_response, dict):
        raise RuntimeError("StockX token refresh returned an unexpected response")
    store_tokens(token_response)
    return access_token()


def catalog_search(
    session: requests.Session,
    token: str,
    query: str,
    page_number: int,
    page_size: int,
) -> requests.Response:
    try:
        return session.get(
            CATALOG_SEARCH_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "x-api-key": required_env("STOCKX_API_KEY"),
                "Accept": "application/json",
            },
            params={
                "query": query,
                "pageNumber": page_number,
                "pageSize": page_size,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.Timeout:
        raise RuntimeError("StockX Catalog Search failed: timeout") from None
    except requests.RequestException:
        raise RuntimeError("StockX Catalog Search failed: network error") from None


def string_value(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_item(item: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    """Map actual Catalog Search product fields to the common Raw schema."""
    return {
        "platform": "stockx",
        "external_product_id": string_value(item.get("productId")),
        "product_name": string_value(item.get("title")),
        "category": string_value(item.get("productType")),
        "brand": string_value(item.get("brand")),
        "currency": None,
        "price": None,
        "original_price": None,
        "discount_percent": None,
        "rating": None,
        "review_count": None,
        "sold_count": None,
        "seller_name": None,
        # Catalog Search supplies urlKey, not an API-provided URL field.
        # Do not construct a web URL from it without an API contract.
        "product_url": None,
        "image_url": None,
        "collected_at": collected_at,
        "source_updated_at": None,
        "ingestion_timestamp": collected_at,
        "raw_source": "stockx_catalog_search_api",
    }


def main() -> None:
    load_local_env()
    queries = search_queries()
    page_size = positive_int_env("STOCKX_PAGE_SIZE", default=25, maximum=MAX_PAGE_SIZE)
    max_pages = positive_int_env("STOCKX_MAX_PAGES", default=2, maximum=MAX_PAGES)
    delay_seconds = request_delay_seconds()
    required_env("STOCKX_API_KEY")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)
    records: list[dict[str, Any]] = []
    seen_product_ids: set[str] = set()
    request_count = 0
    last_request_at: float | None = None
    token = access_token()

    with requests.Session() as session:
        for query in queries:
            query_count = 0
            print(f"Searching StockX query: {query}")

            for page_number in range(1, max_pages + 1):
                if last_request_at is not None:
                    wait_seconds = delay_seconds - (time.monotonic() - last_request_at)
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)

                response = catalog_search(
                    session,
                    token,
                    query,
                    page_number,
                    page_size,
                )
                request_count += 1
                last_request_at = time.monotonic()

                # A single refresh and retry is permitted only for an expired
                # access token. Any subsequent 401 stops the run safely.
                if response.status_code == 401:
                    print("StockX Catalog Search returned HTTP 401; refreshing token once.")
                    token = refresh_access_token()
                    time.sleep(delay_seconds)
                    response = catalog_search(
                        session,
                        token,
                        query,
                        page_number,
                        page_size,
                    )
                    request_count += 1
                    last_request_at = time.monotonic()

                if response.status_code in {401, 403, 429}:
                    raise RuntimeError(
                        "StockX Catalog Search stopped safely: "
                        f"HTTP {response.status_code}"
                    )
                if not response.ok:
                    raise RuntimeError(
                        "StockX Catalog Search failed: "
                        f"HTTP {response.status_code}"
                    )

                try:
                    payload = response.json()
                except ValueError:
                    raise RuntimeError("StockX Catalog Search returned invalid JSON") from None
                if not isinstance(payload, dict):
                    raise RuntimeError("StockX Catalog Search returned an unexpected response")

                products = payload.get("products") or []
                if not isinstance(products, list):
                    raise RuntimeError("StockX Catalog Search returned an unexpected product list")
                if not products:
                    break

                for item in products:
                    if not isinstance(item, dict):
                        continue
                    product_id = string_value(item.get("productId"))
                    if not product_id or product_id in seen_product_ids:
                        continue
                    record = normalize_item(item, collected_at)
                    if record["product_name"]:
                        records.append(record)
                        seen_product_ids.add(product_id)
                        query_count += 1

                if not payload.get("hasNextPage", False):
                    break

            print(f"StockX query '{query}' unique records: {query_count:,}")

    if not records:
        raise RuntimeError("StockX returned no valid records for the configured searches")

    frame = pd.DataFrame(records).drop_duplicates(
        subset=["platform", "external_product_id"]
    )
    output_file = OUTPUT_DIR / f"stockx_{collected_at:%Y-%m-%d_%H%M%S}.parquet"
    table = pa.Table.from_pandas(frame, schema=RAW_SCHEMA, preserve_index=False)
    pq.write_table(
        table,
        output_file,
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )

    print(f"StockX Catalog Search requests: {request_count:,}")
    print(f"StockX unique records written: {len(frame):,}")
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    main()
