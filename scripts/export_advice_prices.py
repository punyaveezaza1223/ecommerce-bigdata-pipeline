import csv
import json
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path("/workspace")
SOURCE = ROOT / "data/raw/external/advice_th/advice_th_20261011_164306_300987.json"
DEST = ROOT / "data/processed_test/advice_th_snapshot_import_v1.csv"

print("ADVICE PRICE EXPORT START", flush=True)

if not SOURCE.is_file():
    raise RuntimeError(f"Source missing: {SOURCE}")

if DEST.exists():
    raise RuntimeError(f"Output exists: {DEST}")

with SOURCE.open("r", encoding="utf-8-sig") as f:
    data = json.load(f)

products = data["products"]

if len(products) != 196:
    raise RuntimeError(f"Expected 196 products, got {len(products)}")

records = []
seen = set()
observed_times = set()

for p in products:
    pid = p["external_product_id"]
    price = Decimal(str(p["price"]))
    observed = p["observed_at_utc"]
    updated = p.get("price_list_last_update") or ""
    source_page = p["source_page"]

    if p["source_system"] != "advice_th":
        raise RuntimeError(f"Wrong source: {pid}")
    if not re.fullmatch(r"A[0-9]+", pid):
        raise RuntimeError(f"Invalid product ID: {pid}")
    if pid in seen:
        raise RuntimeError(f"Duplicate product ID: {pid}")
    if p["currency"] != "THB":
        raise RuntimeError(f"Invalid currency: {pid}")
    if not price.is_finite() or price <= 0 or price > Decimal("9999999999.99"):
        raise RuntimeError(f"Invalid price: {pid}")
    if price.as_tuple().exponent < -2:
        raise RuntimeError(f"Too many decimal places: {pid}")
    if not source_page.startswith("https://www.advice.co.th/"):
        raise RuntimeError(f"Invalid source page: {pid}")

    dt = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise RuntimeError(f"Timestamp has no timezone: {pid}")

    if updated:
        datetime.fromisoformat(updated)

    seen.add(pid)
    observed_times.add(observed)

    records.append([
        pid,
        format(price, ".2f"),
        "THB",
        observed,
        updated,
        source_page
    ])

if len(observed_times) != 1:
    raise RuntimeError("Expected one observation timestamp per collection")

DEST.parent.mkdir(parents=True, exist_ok=True)

with DEST.open("x", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "source_product_id",
        "price_thb",
        "currency",
        "observed_at_utc",
        "price_list_last_update",
        "source_page"
    ])
    writer.writerows(records)

print("CSV_EXPORT_SUCCESS:", DEST, flush=True)
print("PRICE_ROWS:", len(records), flush=True)
print("OBSERVED_AT_UTC:", next(iter(observed_times)), flush=True)
print("PRICE_SAMPLE:", records[:3], flush=True)
