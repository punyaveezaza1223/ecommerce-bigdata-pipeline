# Airflow Final Integration Report

Date: 2026-10-12
Safety boundary: no DAG was triggered, no external API was called, and no
production warehouse row or Docker volume was changed.

## 1. Architecture before and after

### Before

`01_ingest_external_data` had only eBay, Rakuten, and StockX tasks.  It could
only execute code under `/opt/airflow/ingestion`.  Etsy and Advice collectors
were repository scripts using `/workspace/data`, a path unavailable in Airflow.
`02_spark_etl` and `03_load_warehouse` were independent manual DAGs.

### After

```text
00_manual_pipeline_orchestration (manual only; max_active_runs=1)
  -> 01_ingest_external_data (manual only; max_active_runs=1)
       ingest_ebay       ingest_rakuten       ingest_stockx
       ingest_etsy       ingest_advice_th -> prepare_advice_th
  -> 02_spark_etl (manual only)
  -> 03_load_warehouse (manual only)

04_amazon_historical_catalog (manual only, isolated)
  -> validate_historical_catalog_source
```

The controller uses `TriggerDagRunOperator(wait_for_completion=True)` and
treats an unsuccessfully completed child DAG as failure.  It does not schedule
or reset any child run. Existing individual DAGs remain available.

## 2. Etsy integration

- `scripts/collect_etsy.py` and `scripts/collect_etsy_bulk.py` now derive the
  data root from `PIPELINE_DATA_ROOT` (default remains `/workspace/data` for
  backward compatibility).
- Airflow mounts `./scripts` read-only at `/opt/airflow/scripts`; scheduler
  sets `PIPELINE_DATA_ROOT=/opt/airflow/data`.
- `ingest_etsy` calls the existing bounded bulk collector with its existing
  checkpoint, retries, atomic writes, rate limits, and a bounded request/product
  count.
- `ETSY_API_KEY` and `ETSY_SHARED_SECRET` are injected only from the untracked
  environment; neither is in a DAG or report.
- Existing `spark/spark_clean.py` already reads Etsy Parquet and the existing
  warehouse loader remains idempotent for facts.

Runtime API collection was intentionally not executed.

## 3. Advice Thailand integration

- `collect_advice_th.py` now uses `PIPELINE_DATA_ROOT` and retains its robots,
  product ID, THB price, minimum-count, free-disk and exclusive-file checks.
- New `scripts/prepare_advice_th_production.py` is network/database-free. It
  validates each raw JSON snapshot and produces a non-overwriting canonical
  Parquet directory under `raw/external/advice_th_parquet`.
- `prepare_advice_th` follows `ingest_advice_th` in DAG 01.
- `spark/spark_clean.py` now accepts this canonical Advice Parquet directory.
  The existing warehouse loader resolves `advice_th` through `dim_platform`;
  read-only verification confirms that platform already exists.

No Advice scrape, Parquet write, Spark ETL, or warehouse load was run during
this implementation.

## 4. Amazon 2023 historical catalog

`04_amazon_historical_catalog` is separate, manual-only and paused on creation.
Its single task performs a read-only source preflight against either official
gzip metadata or the existing normalized historical Parquet. It has no edge to
the snapshot ETL or `fact_external_product` loader and explicitly creates no
observation date or price fact.

The existing full import tooling remains approval-gated: its final loader is
`scripts/load_amazon_2023.ps1`, which is Windows/PowerShell-based and was not
converted to a new automatic Airflow warehouse writer. Therefore the new DAG
is **container-verified for historical-source validation**, not full catalog
backfill runtime. This preserves the existing 1,722,496 dimension rows and zero
Amazon facts without risk of duplicate import.

## 5. Files changed

- `docker-compose.yml`
- `dags/00_manual_pipeline_orchestration_dag.py` (new)
- `dags/01_ingest_data_dag.py`
- `dags/04_amazon_historical_catalog_dag.py` (new)
- `scripts/collect_etsy.py`
- `scripts/collect_etsy_bulk.py`
- `scripts/collect_advice_th.py`
- `scripts/prepare_advice_th_production.py` (new)
- `scripts/validate_amazon_historical_catalog.py` (new)
- `spark/spark_clean.py`

## 6. Test results and evidence

| Test | Result | Evidence |
| --- | --- | --- |
| Code implementation | PASS | New DAGs and adapters parse in Airflow Python. |
| Docker Compose config | PASS | `docker compose config --quiet` succeeded before service recreation. |
| Airflow DAG parse | PASS | `airflow dags list-import-errors` returned `No data found`. |
| Task visibility | PASS | DAG 01 lists `ingest_ebay`, `ingest_rakuten`, `ingest_stockx`, `ingest_etsy`, `ingest_advice_th`, `prepare_advice_th`; controller and Amazon task are visible. |
| Container paths | PASS | Scheduler has `/opt/airflow/scripts`, `/opt/airflow/data`, and Etsy/Advice/Amazon raw paths. |
| Syntax validation | PASS | Read-only `compile()` validation passed for 11 changed DAG/script/Spark files. |
| Advice schema validation | PASS | Existing raw snapshot validated as 196 canonical THB product rows. |
| Warehouse loader compatibility (read-only) | PASS | `advice_th` exists in `dim_platform` (type `retailer`); existing facts prove the platform mapping resolves. Etsy has 82,683 facts; Amazon has 0 facts. |
| Amazon isolation/source validation | PASS | Existing normalized Amazon historical source contains 11 Parquet files; validation reports no price fact/date creation. |
| Airflow scheduler/webserver | PASS | Both are healthy after recreation; `http://localhost:8190/health` reports healthy metadata and scheduler. |
| Backend regression test suite | NOT TESTED | No backend test suite exists in the repository. Backend health check passed: `http://localhost:8088/api/health` returned `{"status":"ok"}`. |
| Frontend TypeScript | NOT TESTED | No frontend code changed. |
| External API execution | NOT TESTED | Explicitly prohibited for this implementation. |
| Production warehouse load | NOT TESTED | Explicitly prohibited for this implementation. |
| Full pipeline end-to-end | NOT VERIFIED | Would require a deliberate manual pipeline run. |

## 7. Remaining issues / production-validation commands

Before a production run, a maintainer must confirm Etsy credentials and source
permission, confirm Advice robots policy and page contract, and approve the
write scope. Then use the Airflow UI to trigger only
`00_manual_pipeline_orchestration`; it preserves the order 01 → 02 → 03 and
stops on child-DAG failure. Do not run it concurrently with another pipeline
run.

For Amazon, first use `04_amazon_historical_catalog` for source validation.
Any catalog re-import requires separate approval of the existing PowerShell
dimension-only loader or a reviewed container-native replacement. Never attach
Amazon to `03_load_warehouse` or `fact_external_product`.

## 8. Presentation readiness

Airflow now visibly documents the five current marketplace routes in DAG 01:
eBay, Rakuten, StockX, Etsy, and Advice Thailand. The graph also distinguishes
the Advice normalization step and shows the manual controller that enforces
pipeline order. Amazon 2023 is visibly separated as historical catalog
validation, avoiding the inaccurate claim that it is a current price snapshot.
