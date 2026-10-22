import csv
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path("/workspace")
SOURCE = ROOT / "data/raw/external/advice_th/advice_th_20261011_164306_300987.json"
OUTPUT = ROOT / "data/processed_test/advice_th_prices_safe_v1.csv"

print("ADVICE PRICE EXPORT START", flush=True)

if not SOURCE.is_file():
    raise RuntimeError(f"Raw JSON missing: {SOURCE}")

if OUTPUT.exists():
    raise RuntimeError(f"Output already exists: {OUTPUT}")

with SOURCE.open("r", encoding="utf-8-sig") as f:
    data = json.load(f)

products = data.get("products", [])

if not isinstance(products, list) or len(products) != 196:
    raise RuntimeError(
        f"Expected 196 raw products, found {len(products)}"
    )

columns = [
    "source_product_id",
    "price_thb",
    "observed_at_utc",
    "price_list_last_update",
    "source_page",
]

records = []
seen = set()

for product in products:
    product_id = str(product.get("external_product_id") or "").strip()
    observed = str(product.get("observed_at_utc") or "").strip()
    updated = str(product.get("price_list_last_update") or "").strip()
    source_page = str(product.get("source_page") or "").strip()

    if product.get("source_system") != "advice_th":
        raise RuntimeError(f"Wrong source for {product_id}")

    if product.get("currency") != "THB":
        raise RuntimeError(f"Wrong currency for {product_id}")

    if not re.fullmatch(r"A[0-9]+", product_id):
        raise RuntimeError(f"Invalid product ID: {product_id}")

    if product_id in seen:
        raise RuntimeError(f"Duplicate product ID: {product_id}")

    seen.add(product_id)

    try:
        price = Decimal(str(product["price"]))
    except (KeyError, InvalidOperation, TypeError):
        raise RuntimeError(f"Invalid price for {product_id}")

    if not price.is_finite() or price <= 0:
        raise RuntimeError(f"Nonpositive price for {product_id}")

    if not observed or not updated or not source_page:
        raise RuntimeError(f"Missing metadata for {product_id}")

    try:
        datetime.fromisoformat(observed.replace("Z", "+00:00"))
        datetime.fromisoformat(updated)
    except ValueError:
        raise RuntimeError(f"Invalid date for {product_id}")

    if not source_page.startswith("https://www.advice.co.th/"):
        raise RuntimeError(f"Unexpected source page for {product_id}")

    records.append({
        "source_product_id": product_id,
        "price_thb": format(price, "f"),
        "observed_at_utc": observed,
        "price_list_last_update": updated,
        "source_page": source_page,
    })

if len(records) != 196:
    raise RuntimeError("Validation failed")

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# Create exclusively: never overwrite another CSV.
with OUTPUT.open("x", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(records)

print("PRICE_CSV_SUCCESS:", OUTPUT, flush=True)
print("ROWS:", len(records), flush=True)
print("COLUMNS:", ",".join(columns), flush=True)

for record in records[:3]:
    print(
        "SAMPLE:",
        record["source_product_id"],
        record["price_thb"],
        "THB",
        flush=True
    )

print("DONE - NO DATABASE WRITES", flush=True)
