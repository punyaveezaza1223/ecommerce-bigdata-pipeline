# Project Structure

This repository is organized around the runtime data path:

`ingestion → data/raw → spark → data/processed → PostgreSQL warehouse → web / Power BI`

| Path | Purpose | Runtime handling |
|---|---|---|
| `config/` | Non-secret configuration and user-maintained keyword files | Keep paths stable |
| `dags/` | Airflow orchestration definitions | Mounted into Airflow; do not move |
| `ingestion/` | External and internal ingestion programs | Called by DAG absolute container paths |
| `spark/` | Spark ETL and warehouse load programs | Mounted at `/opt/spark/jobs`; do not move |
| `sql/` | Internal/Warehouse schemas, validation and Power BI views | Source-of-truth SQL; backups retained locally |
| `data/` | Runtime data lake: raw, bronze, silver, processed, checkpoints and tests | Generated/local data; ignored by Git; do not relocate |
| `web/backend/` | FastAPI service | Docker build context |
| `web/frontend/` | React/Vite client | Docker build context |
| `dashboards/` | PBIX, preview and refresh guide | Preserve PBIX path for presentation/refresh |
| `tests/` | Automated Python tests | Safe for unit/integration additions |
| `reports/` | Audit, QA, bug, Power BI and presentation documents | Source documentation; preserve historical reports |
| `scripts/` | Utilities, controlled imports, probes and one-off operations | Review each script before execution |
| `logs/` | Runtime Airflow logs | Generated/ignored; do not commit |

## Data classifications

- **Priced snapshots:** eBay, Etsy, Rakuten and Advice Thailand.
- **Catalog-only:** StockX in the current authorized source; price/currency are NULL and must not be inferred.
- **Historical metadata:** Amazon 2023 in `dim_product`; it is not a current price snapshot.

## Safety notes

- `.env`, `.stockx-secrets/`, local OAuth tokens, generated data, logs, Node modules and Vite cache are ignored.
- `temp_lazada/`, `temp_shopee/` and `data/processed_test/` are retained because scripts still reference them. They are not presentation paths and must not be removed/moved without a separate reference audit.
- Docker Compose mounts `./data`, `./spark`, `./dags`, `./ingestion`, `./logs`, `./config` and `.stockx-secrets`; preserve these roots.
