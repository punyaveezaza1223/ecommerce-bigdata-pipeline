"""Resumable, budget-capped Rakuten Raw ingestion."""
from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "data/raw/external/rakuten"
URL = "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701"
MAX_HITS, MAX_PAGES, TIMEOUT = 30, 100, 30
RAW_COLUMNS = ["platform", "external_product_id", "product_name", "category", "brand", "currency", "price", "original_price", "discount_percent", "rating", "review_count", "sold_count", "seller_name", "product_url", "image_url", "collected_at", "source_updated_at", "ingestion_timestamp", "raw_source"]


def env_int(name: str, maximum: int, default: int | None = None) -> int:
    raw = os.getenv(name, "").strip() or (str(default) if default else "")
    if not raw:
        raise RuntimeError(f"Missing required environment variable: {name}")
    try: value = int(raw)
    except ValueError as error: raise RuntimeError(f"{name} must be an integer") from error
    if not 1 <= value <= maximum: raise RuntimeError(f"{name} must be between 1 and {maximum}")
    return value


def nonnegative_env(name: str, maximum: int, default: int) -> int:
    raw = os.getenv(name, "").strip() or str(default)
    try: value = int(raw)
    except ValueError as error: raise RuntimeError(f"{name} must be an integer") from error
    if not 0 <= value <= maximum: raise RuntimeError(f"{name} must be between 0 and {maximum}")
    return value


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value: raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.flush(); os.fsync(handle.fileno())
    temporary.replace(path)


def empty_checkpoint() -> dict[str, Any]: return {"version": 2, "entries": {}, "queries": {}}


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists(): return empty_checkpoint()
    old = json.loads(path.read_text(encoding="utf-8"))
    if old.get("version") == 2:
        old.setdefault("entries", {}); old.setdefault("queries", {})
        return old
    # Legacy state explicitly says completed and identifies a durable Raw file.
    state = empty_checkpoint()
    for keyword in dict.fromkeys(old.get("completed_queries", [])):
        state["entries"][f"{keyword}\t1"] = {"keyword": keyword, "page": 1, "state": "persisted", "raw_file": old.get("raw_file"), "persisted_at": old.get("completed_at")}
        state["queries"][keyword] = {"state": "completed", "last_page": 1, "raw_file": old.get("raw_file"), "completed_at": old.get("completed_at")}
    return state


def rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(encoding="utf-8", newline="") as handle:
        return [r for r in csv.DictReader(handle) if r.get("keyword_ja", "").strip()]


def persisted_pages(state: dict[str, Any], keyword: str) -> set[int]:
    return {int(v["page"]) for v in state["entries"].values() if v.get("keyword") == keyword and v.get("state") == "persisted"}


def select_work(csv_path: Path, state: dict[str, Any], max_pages: int, limit: int) -> list[tuple[str, int]]:
    """Checkpoint is authoritative; CSV status never decides whether to skip."""
    selected, seen = [], set()
    for row in rows(csv_path):
        keyword = row["keyword_ja"].strip()
        if keyword in seen: continue
        seen.add(keyword)
        if state["queries"].get(keyword, {}).get("state") == "completed": continue
        page = 1
        while page in persisted_pages(state, keyword): page += 1
        if page <= max_pages: selected.append((keyword, page))
        if len(selected) == limit: break
    return selected


def value_number(value: Any) -> float | None:
    try: return float(value) if value is not None else None
    except (TypeError, ValueError): return None


def image_url(images: Any) -> str | None:
    if not isinstance(images, list) or not images: return None
    image = images[0]
    if isinstance(image, str): return image.strip() or None
    if isinstance(image, dict):
        for key in ("imageUrl", "url", "image_url"):
            if isinstance(image.get(key), str) and image[key].strip(): return image[key].strip()
    return None


def normalized(item: dict[str, Any], now: datetime) -> dict[str, Any]:
    return {"platform":"rakuten", "external_product_id":item.get("itemCode"), "product_name":item.get("itemName"), "category":str(item["genreId"]) if item.get("genreId") is not None else None, "brand":None, "currency":"JPY", "price":value_number(item.get("itemPrice")), "original_price":None, "discount_percent":None, "rating":value_number(item.get("reviewAverage")), "review_count":item.get("reviewCount"), "sold_count":None, "seller_name":item.get("shopName"), "product_url":item.get("affiliateUrl") or item.get("itemUrl"), "image_url":image_url(item.get("mediumImageUrls")) or image_url(item.get("smallImageUrls")), "collected_at":now, "source_updated_at":None, "ingestion_timestamp":now, "raw_source":"rakuten_ichiba_item_search_api"}


class RateLimitError(RuntimeError):
    def __init__(self, retry_after: float | None):
        self.retry_after = retry_after
        suffix = f" retry_after={retry_after:g}" if retry_after is not None else ""
        super().__init__(f"Rakuten API request failed: HTTP 429{suffix}")


def fetch(session: requests.Session, params: dict[str, Any], access_key: str, retries: int, remaining: int, metrics: dict[str, int]) -> dict[str, Any]:
    if remaining < 1: raise RuntimeError("Rakuten HTTP request budget exhausted")
    for attempt in range(min(retries + 1, remaining)):
        metrics["http_attempts"] += 1
        try: response = session.get(URL, params=params, headers={"accessKey": access_key}, timeout=TIMEOUT)
        except requests.RequestException as error:
            if attempt + 1 == min(retries + 1, remaining): raise RuntimeError("Rakuten API request failed after retries") from error
            metrics["retries"] += 1; time.sleep(min(5 * 2**attempt, 60)); continue
        if response.status_code == 429:
            metrics["http_429"] += 1
            try: retry_after = float(response.headers.get("Retry-After", ""))
            except ValueError: retry_after = None
            raise RateLimitError(retry_after)
        if 500 <= response.status_code <= 599:
            if attempt + 1 == min(retries + 1, remaining): raise RuntimeError(f"Rakuten API request failed: HTTP {response.status_code} after retries")
            metrics["retries"] += 1
            time.sleep(min(5 * 2**attempt, 60)); continue
        if not response.ok: raise RuntimeError(f"Rakuten API request failed: HTTP {response.status_code}")
        try: data = response.json()
        except ValueError as error: raise RuntimeError("Rakuten API request failed: invalid JSON response") from error
        if not isinstance(data, dict): raise RuntimeError("Rakuten returned an unexpected response structure")
        metrics["successful_requests"] += 1
        return data
    raise RuntimeError("Rakuten API request budget exhausted")


def append_metrics(path: Path, metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(metrics, ensure_ascii=False, sort_keys=True) + "\n"); handle.flush(); os.fsync(handle.fileno())


def next_output_file(now: datetime) -> Path:
    """Never overwrite an existing Raw batch, including after a fast restart."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    base = OUTPUT_DIR / f"rakuten_{now:%Y-%m-%d_%H%M%S}.parquet"
    if not base.exists(): return base
    suffix = 1
    while True:
        candidate = OUTPUT_DIR / f"rakuten_{now:%Y-%m-%d_%H%M%S}_{suffix}.parquet"
        if not candidate.exists(): return candidate
        suffix += 1


def persist_batch(records: list[dict[str, Any]], entries: list[tuple[str, int]], state: dict[str, Any], checkpoint_path: Path, now: datetime) -> tuple[Path, int]:
    """Write a new Parquet first, then atomically advance the checkpoint."""
    output = next_output_file(now)
    frame = pd.DataFrame(records, columns=RAW_COLUMNS).drop_duplicates(subset=["platform", "external_product_id"])
    frame.to_parquet(output, index=False, engine="pyarrow", coerce_timestamps="ms", allow_truncated_timestamps=True)
    for keyword, page in entries:
        state["entries"][f"{keyword}\t{page}"] = {"keyword":keyword,"page":page,"state":"persisted","raw_file":str(output),"persisted_at":now.isoformat()}
    state["version"], state["updated_at"] = 2, now.isoformat()
    atomic_json(checkpoint_path, state)
    return output, int(frame.external_product_id.nunique())


def main() -> None:
    csv_path = Path(required_env("RAKUTEN_KEYWORD_CSV"))
    checkpoint_path = Path(os.getenv("RAKUTEN_CHECKPOINT_PATH", str(ROOT / "data/checkpoints/rakuten.json")))
    max_pages = env_int("RAKUTEN_MAX_PAGES", MAX_PAGES, 1) if os.getenv("RAKUTEN_MAX_PAGES", "").strip() else env_int("EXTERNAL_MAX_PAGES", MAX_PAGES, 1)
    budget, state = env_int("RAKUTEN_REQUEST_BUDGET", 100000, 10), load_checkpoint(checkpoint_path)
    work = select_work(csv_path, state, max_pages, budget)
    if os.getenv("RAKUTEN_DRY_RUN", "false").lower() == "true":
        print(f"Rakuten dry-run: {len(rows(csv_path)):,} CSV keywords; selected {len(work)}; HTTP budget: {budget}")
        for keyword, page in work: print(f"PREVIEW keyword={keyword} start_page={page}")
        return
    app_id, key = required_env("RAKUTEN_APPLICATION_ID"), required_env("RAKUTEN_ACCESS_KEY")
    page_size, retries = env_int("RAKUTEN_PAGE_SIZE", MAX_HITS), nonnegative_env("RAKUTEN_MAX_RETRIES", 10, 5)
    delay = float(os.getenv("RAKUTEN_REQUEST_DELAY_SECONDS", "2"))
    now, records, entries = datetime.now(timezone.utc).replace(microsecond=0), [], []
    metrics: dict[str, int] = {"http_attempts":0,"successful_requests":0,"http_429":0,"retries":0}
    rate_limit: RateLimitError | None = None
    interrupted = False
    output_files: list[str] = []

    def flush() -> None:
        """Commit all successful pages gathered since the previous checkpoint."""
        nonlocal records, entries
        if not entries:
            return
        output, unique_ids = persist_batch(records, entries, state, checkpoint_path, now)
        output_files.append(str(output))
        print(f"Checkpoint saved: {len(entries)} pages, {unique_ids} unique IDs, {output}")
        records, entries = [], []

    try:
        with requests.Session() as session:
            for keyword, page in work:
                terminal = False
                while page <= max_pages and metrics["http_attempts"] < budget:
                    if metrics["http_attempts"]: time.sleep(delay)
                    data = fetch(session, {"applicationId":app_id,"keyword":keyword,"format":"json","formatVersion":2,"hits":page_size,"page":page,"elements":"itemCode,itemName,itemPrice,itemUrl,affiliateUrl,smallImageUrls,mediumImageUrls,genreId,shopName,reviewAverage,reviewCount"}, key, retries, budget - metrics["http_attempts"], metrics)
                    items = data.get("items") or data.get("Items") or []
                    if not isinstance(items, list): raise RuntimeError("Rakuten returned an unexpected item list")
                    records.extend(r for r in (normalized(i, now) for i in items) if r["external_product_id"] and r["product_name"] and r["price"] is not None)
                    entries.append((keyword, page)); page_count = data.get("pageCount")
                    terminal = not items or len(items) < page_size or (page_count is not None and page >= int(page_count)) or page >= max_pages
                    page += 1
                    # A checkpoint batch is based on successful HTTP responses,
                    # not attempts; retries never falsely advance progress.
                    if len(entries) >= 10:
                        flush()
                    if terminal: break
                if terminal:
                    state["queries"][keyword] = {"state":"completed", "last_page":page-1}
                elif entries and entries[-1][0] == keyword:
                    state["queries"][keyword] = {"state":"partial", "last_page":page-1}
    except RateLimitError as error:
        rate_limit = error
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupted; saving successful pages before exit...")
    finally:
        # Includes valid zero-item responses: an empty but schema-correct
        # Parquet is durable evidence before their checkpoint is advanced.
        for keyword, page in entries:
            state["queries"].setdefault(keyword, {"state":"partial", "last_page":page})
        flush()
        # Query completion metadata may have changed after the last 10-page
        # flush; it refers only to pages already durably written above.
        atomic_json(checkpoint_path, state)

    unique_ids = len(set().union(*(set(pd.read_parquet(file_name).external_product_id.dropna().astype(str)) for file_name in output_files))) if output_files else 0
    result: dict[str, Any] = {**metrics,"unique_raw_ids":unique_ids,"output_file":output_files[-1] if output_files else None,"output_files":output_files,"completed_checkpoint_entries":sum(1 for entry in state["entries"].values() if entry.get("persisted_at") == now.isoformat()),"recorded_at":now.isoformat(),"stopped_reason":"http_429" if rate_limit else "keyboard_interrupt" if interrupted else None,"retry_after_seconds":rate_limit.retry_after if rate_limit else None}
    append_metrics(Path(os.getenv("RAKUTEN_METRICS_PATH", str(checkpoint_path.with_name("rakuten_metrics.jsonl")))), result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if rate_limit: raise rate_limit
    if interrupted: raise KeyboardInterrupt


if __name__ == "__main__": main()
