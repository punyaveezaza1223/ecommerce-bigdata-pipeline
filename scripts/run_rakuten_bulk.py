"""Safe host-side orchestrator for resumable Rakuten micro-batches.

It is intentionally dry-run by default.  A live run needs an explicit numeric
daily limit because Rakuten's public Item Search documentation does not publish
one.  This program never supplies or prints credentials.
"""
from __future__ import annotations

import argparse
import atexit
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
CHECKPOINT = ROOT / "data/checkpoints/rakuten.json"
METRICS = ROOT / "data/checkpoints/rakuten_metrics.jsonl"
SCHEDULER = "pipeline-airflow-scheduler"
MICRO_BATCH_ATTEMPTS = 10
MIN_FREE_GB = 10
LOCK_PATH = ROOT / "data/checkpoints/rakuten_bulk.lock"
COOLDOWN_PATH = ROOT / "data/checkpoints/rakuten_bulk_cooldown.json"


@dataclass(frozen=True)
class Decision:
    proceed: bool
    reason: str


def latest_metrics(path: Path = METRICS) -> dict:
    if not path.exists():
        return {}
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else {}


def metric_attempts_for_day(day: str, path: Path = METRICS) -> tuple[int, bool]:
    """Return logged attempts only; False means metrics cannot prove full-day usage."""
    if not path.exists():
        return 0, False
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    attempts = sum(int(item.get("http_attempts", 0)) for item in records if str(item.get("recorded_at", "")).startswith(day))
    # Metrics were introduced after earlier pilot runs, so a file alone cannot
    # establish provider-side daily usage.
    return attempts, False


def checkpoint_counts(path: Path = CHECKPOINT) -> tuple[int, int]:
    state = json.loads(path.read_text(encoding="utf-8"))
    completed = sum(v.get("state") == "completed" for v in state.get("queries", {}).values())
    return completed, 3000 - completed


def decide(free_gb: float, available_ram_gb: float, run_attempts: int,
           max_total_attempts: int, attempts_today: int, daily_limit: int | None,
           saw_429: bool) -> Decision:
    if free_gb < MIN_FREE_GB:
        return Decision(False, "disk safety floor reached")
    if available_ram_gb <= 0:
        return Decision(False, "available RAM could not be confirmed")
    if saw_429:
        return Decision(False, "HTTP 429 observed; stop and honour provider cooldown")
    if run_attempts >= max_total_attempts:
        return Decision(False, "operational max-total-attempts reached")
    if daily_limit is not None and attempts_today >= daily_limit:
        return Decision(False, "verified daily request limit would be exceeded")
    return Decision(True, "safe to start one micro-batch")


def run(command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=check)


def docker_resource_state(execute: Callable[[list[str]], subprocess.CompletedProcess[str]] = run) -> tuple[float, float]:
    disk = shutil.disk_usage(ROOT).free / 1024**3
    result = execute(["docker", "exec", SCHEDULER, "sh", "-c", "free -b | awk '/Mem:/ {print $7}'"])
    ram = float(result.stdout.strip()) / 1024**3
    return disk, ram


def dags_idle(execute: Callable[[list[str]], subprocess.CompletedProcess[str]] = run) -> bool:
    for dag in ("02_spark_etl", "03_load_warehouse"):
        result = execute(["docker", "exec", SCHEDULER, "airflow", "dags", "list-runs", "-d", dag, "--state", "running"])
        if "No data found" not in result.stdout:
            return False
    return True


def invoke_ingestion(batch_budget: int, execute: Callable[[list[str]], subprocess.CompletedProcess[str]] = run) -> None:
    # Bulk policy does not retry 429: the first 429 ends this run safely.
    execute(["docker", "exec", "-e", "RAKUTEN_DRY_RUN=false", "-e", f"RAKUTEN_REQUEST_BUDGET={batch_budget}", "-e", "RAKUTEN_PAGE_SIZE=30", "-e", "RAKUTEN_MAX_PAGES=1", "-e", "RAKUTEN_REQUEST_DELAY_SECONDS=5", "-e", "RAKUTEN_MAX_RETRIES=0", SCHEDULER, "python", "/opt/airflow/ingestion/external/rakuten_ingestion.py"])


def trigger_and_wait(dag: str, execute: Callable[[list[str]], subprocess.CompletedProcess[str]] = run) -> None:
    execute(["docker", "exec", SCHEDULER, "airflow", "dags", "trigger", dag])
    # The CLI is queried rather than assuming that trigger implies success.
    for _ in range(120):
        state = execute(["docker", "exec", SCHEDULER, "airflow", "dags", "list-runs", "-d", dag, "-o", "json"]).stdout
        runs = json.loads(state)
        latest = runs[0].get("state") if runs else ""
        if dag_finished(dag, latest): return
        time.sleep(10)
    raise RuntimeError(f"{dag} did not finish within 20 minutes")


def dag_finished(dag: str, state: str) -> bool:
    """Return only verified success; surface a failure to stop bulk execution."""
    if state == "success": return True
    if state in {"failed", "upstream_failed"}: raise RuntimeError(f"{dag} failed")
    return False


def warehouse_marketplace_total(execute: Callable[[list[str]], subprocess.CompletedProcess[str]] = run) -> int:
    result = execute(["docker", "exec", "pipeline-warehouse-postgres", "psql", "-U", "warehouse", "-d", "ecommerce_warehouse", "-At", "-c", "SELECT COUNT(*) FROM dim_product WHERE source_system IN ('ebay','rakuten','stockx')"])
    return int(result.stdout.strip())


def progress_report(daily_limit: int | None, max_total_attempts: int | None = None, warehouse_total: int | None = None) -> str:
    completed, pending = checkpoint_counts()
    metrics = latest_metrics()
    free = shutil.disk_usage(ROOT).free / 1024**3
    logged_attempts, coverage_complete = metric_attempts_for_day(datetime.now(timezone.utc).date().isoformat())
    return json.dumps({"completed_keywords": completed, "pending_keywords": pending,
        "latest_http_attempts": metrics.get("http_attempts", 0), "latest_http_429": metrics.get("http_429", 0),
        "latest_unique_raw_ids": metrics.get("unique_raw_ids", 0), "disk_free_gb": round(free, 2),
        "warehouse_marketplace_products": warehouse_total,
        "remaining_to_500000": 500000 - warehouse_total if warehouse_total is not None else None,
        "metrics_attempts_today_logged": logged_attempts,
        "metrics_can_prove_full_day_usage": coverage_complete,
        "operational_max_total_attempts": max_total_attempts,
        "verified_rakuten_daily_limit": daily_limit}, ensure_ascii=False)


def acquire_lock(path: Path = LOCK_PATH) -> None:
    """Atomic host lock: one bulk orchestrator at a time."""
    try:
        descriptor = path.open("x", encoding="utf-8")
    except FileExistsError as error:
        raise RuntimeError(f"Another Rakuten bulk instance holds {path}") from error
    descriptor.write(str(os.getpid()))
    descriptor.close()
    atexit.register(lambda: path.unlink(missing_ok=True))


def cooldown_active(path: Path = COOLDOWN_PATH) -> bool:
    if not path.exists(): return False
    until = datetime.fromisoformat(json.loads(path.read_text(encoding="utf-8"))["until"])
    if datetime.now(timezone.utc) < until: return True
    path.unlink(missing_ok=True)
    return False


def record_cooldown(retry_after: float | None, path: Path = COOLDOWN_PATH) -> None:
    """Persist provider Retry-After so a later resume cannot start too early."""
    if retry_after is None: return
    path.parent.mkdir(parents=True, exist_ok=True)
    until = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"until": until.isoformat(), "retry_after_seconds": retry_after}), encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-limit", type=int, help="optional provider-verified Rakuten daily HTTP-attempt limit")
    parser.add_argument("--attempts-today", type=int, default=0, help="provider-verified attempts already consumed today")
    parser.add_argument("--max-total-attempts", type=int, help="internal cap for this run; not a Rakuten quota")
    parser.add_argument("--live", action="store_true", help="perform one or more live micro-batches")
    parser.add_argument("--check-connectivity", action="store_true", help="read-only Docker, PostgreSQL, and Airflow checks")
    parser.add_argument("--max-batches", type=int, default=100000)
    args = parser.parse_args()
    if not args.live:
        warehouse_total = None
        if args.check_connectivity:
            if not dags_idle(): raise RuntimeError("DAG 02 or DAG 03 is already running")
            warehouse_total = warehouse_marketplace_total()
        print(progress_report(args.daily_limit, args.max_total_attempts, warehouse_total)); return
    if not args.max_total_attempts or args.max_total_attempts < 1:
        raise SystemExit("Live mode requires --max-total-attempts (an internal safety cap)")
    if args.daily_limit is not None and args.attempts_today > args.daily_limit:
        raise SystemExit("--attempts-today cannot exceed verified --daily-limit")
    acquire_lock()
    if cooldown_active():
        raise RuntimeError("Rakuten Retry-After cooldown is still active")
    attempts = args.attempts_today
    run_attempts = 0
    for _ in range(args.max_batches):
        completed, pending = checkpoint_counts()
        if not pending: break
        if not dags_idle(): raise RuntimeError("DAG 02 or DAG 03 is already running")
        free, ram = docker_resource_state()
        recent = latest_metrics()
        decision = decide(free, ram, run_attempts, args.max_total_attempts, attempts, args.daily_limit, recent.get("http_429", 0) > 0)
        if not decision.proceed: raise RuntimeError(decision.reason)
        batch_budget = min(MICRO_BATCH_ATTEMPTS, args.max_total_attempts - run_attempts)
        if args.daily_limit is not None:
            batch_budget = min(batch_budget, args.daily_limit - attempts)
        try:
            invoke_ingestion(batch_budget)
        except subprocess.CalledProcessError:
            recent = latest_metrics()
            if recent.get("http_429", 0):
                record_cooldown(recent.get("retry_after_seconds"))
                raise RuntimeError("HTTP 429 observed; checkpointed work is resumable after Retry-After") from None
            raise
        recent = latest_metrics()
        batch_attempts = int(recent.get("http_attempts", 0))
        attempts += batch_attempts
        run_attempts += batch_attempts
        if recent.get("http_429", 0): raise RuntimeError("HTTP 429 observed; bulk stopped")
        trigger_and_wait("02_spark_etl")
        trigger_and_wait("03_load_warehouse")
        print(progress_report(args.daily_limit, args.max_total_attempts, warehouse_marketplace_total()))


if __name__ == "__main__": main()
