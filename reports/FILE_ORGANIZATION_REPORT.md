# File Organization Report — Minimal Change Audit

## Changes made

- Added `PROJECT_STRUCTURE.md` as the presentation-facing map of the repository.
- Added `node_modules/`, `.vite/` and timestamp-style backup names to `.gitignore`.
- No source, data, PBIX, DAG, Docker mount, schema or secret file was moved/deleted.

## Files/directories not moved

| Location | Reason |
|---|---|
| `dags/`, `ingestion/`, `spark/` | Referenced by Airflow and Docker container paths |
| `data/` | Mounted by Docker and contains raw/processed/checkpoint runtime state |
| `web/` | Docker build contexts and public services |
| `dashboards/ecommerce_analytics.pbix` | Existing Power BI artifact; do not rewrite/move without validating refresh |
| `.stockx-secrets/`, `.env` | Credentials; ignored and not inspected/moved |
| `temp_lazada/`, `temp_shopee/` | Referenced by `scripts/import_hybrid_csv.py` |
| `data/processed_test/` | Referenced by Etsy, Advice and Amazon utility scripts |
| `scripts/*backup*`, `sql/*backup*` | Retained recovery evidence; now ignored if untracked |

## Candidates for a future approved cleanup

1. Archive legacy scraper paths only after proving no command, DAG or presentation guide invokes them.
2. Put one-off probe scripts under a dated `scripts/archive/` only after updating any documentation/reference.
3. Move large obsolete test outputs only after checksum, retention agreement and reference audit.

No candidate was moved in this task because the current references make a blind relocation unsafe.
