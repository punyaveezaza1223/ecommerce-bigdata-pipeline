#!/usr/bin/env python3
"""Etsy Open API v3 bulk collector: permitted, bounded search only.

Persistent SQLite checkpoint and global listing_id dedup; one raw JSON and
one normalized Parquet per successful page. Does not load the warehouse.
"""
import argparse
import hashlib
import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API = "https://api.etsy.com/v3/application/listings/active"
DEFAULT_KEYWORDS = [
    "handmade jewelry", "silver ring", "gold necklace", "beaded bracelet",
    "ceramic mug", "pottery bowl", "wall art", "art print",
    "home decor", "wooden furniture", "handmade candle", "soap",
    "knitted scarf", "crochet bag", "linen dress", "leather wallet",
    "personalized gift", "wedding decor", "baby blanket", "pet accessories",
    "embroidery", "quilting", "vintage clothing", "stationery",
]

SCHEMA_FIELDS = [
    ("platform", "string"), ("external_product_id", "string"),
    ("product_name", "string"), ("category", "string"),
    ("brand", "string"), ("currency", "string"),
    ("price", "float64"), ("original_price", "float64"),
    ("discount_percent", "float64"), ("rating", "float64"),
    ("review_count", "int64"), ("sold_count", "int64"),
    ("seller_name", "string"), ("product_url", "string"),
    ("image_url", "string"), ("collected_at", "string"),
    ("source_updated_at", "string"),
    ("ingestion_timestamp", "string"), ("raw_source", "string"),
]

class OffsetLimitError(Exception):
    """Exception raised when pagination limit is reached (HTTP 400)."""
    pass

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as out:
        json.dump(data, out, ensure_ascii=False)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)


def price_currency(value):
    if not isinstance(value, dict):
        return None, None
    currency = value.get("currency_code")
    try:
        amount = float(value["amount"])
        divisor = float(value["divisor"])
        if divisor <= 0:
            return None, currency
        return amount / divisor, currency
    except (TypeError, KeyError, ValueError, ZeroDivisionError):
        return None, currency


def iso_timestamp(value):
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def normalize(item, collected_at):
    product_id = item.get("listing_id")
    title = item.get("title")
    if product_id is None or not str(product_id).strip() or not str(title or "").strip():
        return None
    price, currency = price_currency(item.get("price"))
    return {
        "platform": "etsy",
        "external_product_id": str(product_id),
        "product_name": str(title),
        "category": None, "brand": None, "currency": currency, "price": price,
        "original_price": None, "discount_percent": None, "rating": None,
        "review_count": None, "sold_count": None, "seller_name": None,
        "product_url": item.get("url"), "image_url": None,
        "collected_at": collected_at,
        "source_updated_at": iso_timestamp(item.get("updated_timestamp")),
        "ingestion_timestamp": collected_at,
        "raw_source": "etsy_open_api_v3_active_bulk",
    }


def database(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("CREATE TABLE IF NOT EXISTS seen (listing_id TEXT PRIMARY KEY)")
    db.execute("""CREATE TABLE IF NOT EXISTS progress (
        keyword TEXT PRIMARY KEY, next_offset INTEGER NOT NULL,
        completed INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL
    )""")
    db.commit()
    return db


def progress_for(db, keyword):
    row = db.execute("SELECT next_offset, completed FROM progress WHERE keyword=?", (keyword,)).fetchone()
    return row if row else (0, 0)


def update_progress(db, keyword, next_offset, completed, listing_ids):
    with db:
        db.executemany("INSERT OR IGNORE INTO seen(listing_id) VALUES (?)", [(x,) for x in listing_ids])
        db.execute("""INSERT INTO progress (keyword, next_offset, completed, updated_at)
            VALUES (?, ?, ?, ?) ON CONFLICT(keyword) DO UPDATE SET
            next_offset=excluded.next_offset, completed=excluded.completed,
            updated_at=excluded.updated_at""", (keyword, next_offset, int(completed), utc_now()))


def fetch_page(key, secret, keyword, offset, limit, remaining_floor):
    url = API + "?" + urlencode({"keywords": keyword, "limit": limit, "offset": offset})
    req = Request(url, headers={"x-api-key": f"{key}:{secret}", "Accept": "application/json", "User-Agent": "EcommerceDataPipelineEtsyCollector/1.0"})
    for attempt in range(4):
        try:
            with urlopen(req, timeout=40) as response:
                remaining_text = response.headers.get("x-remaining-today")
                if remaining_text is None:
                    raise RuntimeError("Missing quota header x-remaining-today; stopping conservatively")
                try:
                    remaining = int(remaining_text)
                except ValueError as exc:
                    raise RuntimeError("Invalid Etsy remaining daily quota header") from exc
                payload = json.load(response)
                if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                    raise RuntimeError("Unexpected Etsy API response: missing results array")
                return payload, remaining
        except HTTPError as exc:
            status = exc.code
            if status == 400:
                # Custom handling: Etsy returns 400 when offset limit is reached
                raise OffsetLimitError("HTTP 400: pagination limit reached") from None
            if status in (401, 403):
                raise RuntimeError(f"Etsy HTTP {status}: check access / request parameters; stopped") from None
            if status == 429:
                retry_header = exc.headers.get("retry-after")
                try:
                    wait = float(retry_header)
                except (ValueError, TypeError):
                    wait = 60.0
                if wait > 120 or attempt == 3:
                    raise RuntimeError(f"Etsy HTTP 429; retry-after={retry_header!r}; stopped to protect quota") from None
                wait = max(2.0, wait)
            elif 500 <= status <= 599 and attempt < 3:
                wait = min(30.0, 2.0 ** (attempt + 1) + random.random())
            else:
                raise RuntimeError(f"Etsy HTTP {status}; stopped") from None
            print(f"Transient HTTP {status}, wait {wait:.1f}s (attempt {attempt+1}/4)", flush=True)
            time.sleep(wait)
        except (URLError, TimeoutError) as exc:
            if attempt == 3:
                raise RuntimeError(f"Network error: {exc}; stopped") from None
            time.sleep(min(30.0, 2.0 ** (attempt + 1) + random.random()))
    raise RuntimeError("Retries exhausted")


def parquet_atomic(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    types = {"string": pa.string(), "float64": pa.float64(), "int64": pa.int64()}
    schema = pa.schema([pa.field(name, types[dtype], nullable=True) for name, dtype in SCHEMA_FIELDS])
    table = pa.Table.from_pylist(rows, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    pq.write_table(table, str(temp), compression="snappy")
    os.replace(temp, path)


def main():
    parser = argparse.ArgumentParser(description="Bounded Etsy bulk collector with persistent checkpoint")
    parser.add_argument("--max-requests", type=int, default=10, help="Maximum successful pages per run (default 10)")
    parser.add_argument("--max-new-products", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--quota-floor", type=int, default=250, help="Stop if daily remaining reaches this amount")
    parser.add_argument("--delay", type=float, default=1.5, help="Minimum seconds between successful pages")
    parser.add_argument("--keywords-file", type=Path, help="UTF-8 file with one search keyword per line")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("PIPELINE_DATA_ROOT", "/workspace/data")),
        help="Data root (default: PIPELINE_DATA_ROOT or /workspace/data)",
    )
    args = parser.parse_args()
    if not 1 <= args.limit <= 100 or not 1 <= args.max_requests <= 500 or not 1 <= args.max_new_products <= 50000:
        parser.error("Limits: --limit 1..100, --max-requests 1..500, --max-new-products 1..50000")
    if args.quota_floor < 100 or args.delay < 1.0:
        parser.error("Safety minimums: --quota-floor >=100, --delay >=1.0")

    key, secret = os.getenv("ETSY_API_KEY"), os.getenv("ETSY_SHARED_SECRET")
    if not key or not secret:
        raise RuntimeError("Missing ETSY_API_KEY or ETSY_SHARED_SECRET (use --env-file .env)")

    words = args.keywords_file.read_text(encoding="utf-8-sig").splitlines() if args.keywords_file else DEFAULT_KEYWORDS
    keywords = list(dict.fromkeys(w.strip() for w in words if w.strip() and not w.lstrip().startswith("#")))
    if not keywords:
        raise RuntimeError("No keywords provided")
    if len(keywords) > 1000:
        raise RuntimeError("Keyword limit exceeded (1000)")

    root = args.root
    raw_json_dir = root / "raw" / "external" / "etsy_bulk_json"
    parquet_dir = root / "raw" / "external" / "etsy"
    checkpoint_path = root / "checkpoints" / "etsy_bulk.sqlite3"
    db = database(checkpoint_path)
    successful = 0
    added = 0
    last_remaining = None
    print(f"Etsy bulk START | keywords={len(keywords)} limit={args.limit} max_requests={args.max_requests} quota_floor={args.quota_floor}", flush=True)
    print(f"Checkpoint: {checkpoint_path}", flush=True)
    try:
        for index, keyword in enumerate(keywords):
            offset, completed = progress_for(db, keyword)
            if completed:
                continue
            while offset <= 12000:
                if successful >= args.max_requests or added >= args.max_new_products:
                    print("STOP: run request/product cap reached", flush=True)
                    return
                # Globally enforce floor for future requests, including keyword changes.
                if last_remaining is not None and last_remaining <= args.quota_floor:
                    print(f"STOP: remaining daily quota {last_remaining} <= floor {args.quota_floor}", flush=True)
                    return

                try:
                    payload, remaining = fetch_page(key, secret, keyword, offset, args.limit, args.quota_floor)
                except OffsetLimitError:
                    print(f"Offset limit reached for {keyword!r} at offset={offset}. Moving to next keyword.", flush=True)
                    update_progress(db, keyword, offset, 1, set())
                    break # ข้ามไปคีย์เวิร์ดถัดไปทันที

                last_remaining = remaining
                results = payload["results"]
                page_rows = []
                page_ids = set()
                collected_at = utc_now()
                for item in results:
                    row = normalize(item, collected_at)
                    if row is None:
                        continue
                    identifier = row["external_product_id"]
                    if identifier in page_ids:
                        continue
                    page_ids.add(identifier)
                    if db.execute("SELECT 1 FROM seen WHERE listing_id=?", (identifier,)).fetchone() is None:
                        page_rows.append(row)

                keyword_hash = hashlib.sha256(keyword.encode("utf-8")).hexdigest()[:16]
                basename = f"k{keyword_hash}_o{offset:05d}"
                atomic_json(raw_json_dir / f"{basename}.json", {
                    "keyword": keyword, "offset": offset, "limit": args.limit,
                    "count": payload.get("count"), "collected_at": collected_at,
                    "results": results,
                })
                if page_rows:
                    parquet_atomic(parquet_dir / f"etsy_bulk_{basename}.parquet", page_rows)
                else:
                    stale = parquet_dir / f"etsy_bulk_{basename}.parquet"
                    if stale.exists():
                        raise RuntimeError(f"Existing page file without new rows: {stale}; manual inspection required")

                next_offset = offset + len(results)
                total = payload.get("count")
                is_done = not results or len(results) < args.limit or next_offset > 12000
                if isinstance(total, int) and next_offset >= total:
                    is_done = True

                update_progress(db, keyword, next_offset, is_done, page_ids)
                successful += 1
                added += len(page_rows)
                print(f"[{successful}/{args.max_requests}] {keyword!r} offset={offset} returned={len(results)} new={len(page_rows)} total_new_run={added} remaining_today={remaining}", flush=True)

                if remaining <= args.quota_floor:
                    print(f"STOP: remaining daily quota {remaining} <= floor {args.quota_floor}", flush=True)
                    return
                if is_done:
                    break
                offset = next_offset
                time.sleep(args.delay)
        print("All provided keywords completed within pagination limits", flush=True)
    finally:
        total_unique = db.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        print(f"RUN SUMMARY: successful_pages={successful} new_this_run={added} cumulative_seen={total_unique} remaining_today={last_remaining}", flush=True)
        db.close()


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, sqlite3.Error) as exc:
        print(f"SAFE STOP: {exc}", file=sys.stderr)
        sys.exit(1)
