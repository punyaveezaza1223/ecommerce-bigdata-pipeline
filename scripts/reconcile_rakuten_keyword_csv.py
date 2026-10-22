"""Host-only synchronization of read-only Rakuten CSV metadata."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def main(csv_path: Path, checkpoint_path: Path) -> None:
    state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if state.get("version") != 2:
        raise RuntimeError("Checkpoint must be version 2")
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fields, rows = reader.fieldnames, list(reader)
    if not fields:
        raise RuntimeError("CSV header is missing")
    completed = partial = 0
    for row in rows:
        record = state["queries"].get(row.get("keyword_ja", ""), {})
        if record.get("state") in {"completed", "partial"}:
            row["status"] = record["state"]
            row["last_page"] = str(record.get("last_page", 0))
            row["last_run_at"] = record.get("completed_at", state.get("updated_at", ""))
            completed += record["state"] == "completed"
            partial += record["state"] == "partial"
    temporary = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows); handle.flush()
    temporary.replace(csv_path)
    print(f"completed={completed} partial={partial} rows={len(rows)}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: reconcile_rakuten_keyword_csv.py CSV CHECKPOINT")
    main(Path(sys.argv[1]), Path(sys.argv[2]))
