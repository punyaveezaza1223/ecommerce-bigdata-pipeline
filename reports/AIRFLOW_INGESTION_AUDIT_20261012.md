# Airflow Ingestion Coverage Audit — 2026-10-12

## Scope and safety

This is a read-only audit. No DAG was triggered and no raw, processed, or
warehouse data was changed.

## Current DAG inventory

| DAG | Current task(s) | Scheduling / relationship |
| --- | --- | --- |
| `01_ingest_external_data` | `ingest_ebay`, `ingest_rakuten`, `ingest_stockx` | Manual (`schedule=None`); its three tasks are independent. |
| `02_spark_etl` | `spark_clean_external` | Manual and separate from ingestion. It runs `spark/spark_clean.py`. |
| `03_load_warehouse` | `load_external_product` | Manual and separate from the other two DAGs. It runs `spark/load_warehouse.py`. |

There is no cross-DAG dependency. The production sequence must currently be
started manually as 01 → 02 → 03; completion of one DAG does not trigger the
next.

## Evidence for each platform

| Platform | Existing ingestion / source path | Production warehouse route | Airflow coverage |
| --- | --- | --- | --- |
| eBay | `ingestion/external/ebay_ingestion.py` | raw → `spark/spark_clean.py` → `spark/load_warehouse.py` | Task in 01 |
| Rakuten | `ingestion/external/rakuten_ingestion.py` | raw → Spark ETL → warehouse load | Task in 01 |
| StockX | `ingestion/external/stockx_ingestion.py` | raw → Spark ETL → warehouse load | Task in 01 |
| Etsy | `scripts/collect_etsy.py` or bounded `scripts/collect_etsy_bulk.py`; raw Parquet is `data/raw/external/etsy` | Raw Parquet is included by `spark/spark_clean.py`, then warehouse loader | No task |
| Advice Thailand | `scripts/collect_advice_th.py`; raw JSON is `data/raw/external/advice_th/advice_th_20261011_164306_300987.json` | Existing import was a validated/manual flow represented by `data/processed_test/advice_th_import_*/advice_import.sql`; it is not included in `spark/spark_clean.py` production inputs | No task |
| Amazon 2023 | `scripts/download_amazon_metadata.py` → `scripts/import_amazon_metadata_spark.py` → `scripts/export_amazon_2023_dim.py` → `scripts/load_amazon_2023.ps1` | Historical catalog import into `dim_product` only | No task (correctly absent from current-price ingestion) |

## Root cause of missing Etsy and Advice tasks

`01_ingest_external_data` was implemented for scripts under the Docker-mounted
`/opt/airflow/ingestion/external` directory. The Etsy and Advice collectors
are under repository `scripts/`, which is **not** mounted into any Airflow
container. They use hard-coded `/workspace/data/...` paths; neither Airflow
nor Spark provides `/workspace` in the running containers. Both collectors
would therefore fail before safely producing input if referenced by a new
`BashOperator` today.

The Etsy bulk collector is otherwise suited to controlled ingestion: it has
request/product caps, SQLite checkpoints, exclusive/atomic writes, retry
limits, and verifies Etsy credentials. Advice also validates `robots.txt`,
valid product ids, THB prices and a minimum product count before writing an
exclusive JSON snapshot. However, Advice currently lacks a production Spark
normalization/load path; its existing `prepare_advice_th.py` deliberately
writes `processed_test`, so it must not be attached to the production loader.

## Amazon 2023 classification

Amazon 2023 is historical metadata, not a current price-snapshot source.
`import_amazon_metadata_spark.py` explicitly writes only inside
`data/processed_test` and describes the reference date as historical. The
PowerShell loader inserts catalog dimensions only. It must remain separate
from `fact_external_product`; the warehouse currently confirms 1,722,496
Amazon catalog products and zero Amazon facts.

If a visible Airflow representation is required, create a separate manual,
paused `amazon_2023_historical_catalog` DAG only after replacing the
Windows-only/machine-path import wrapper with a container-mounted,
idempotent catalog-only task. It must have no dependency into the current
snapshot fact loader.

## Safe implementation plan before adding tasks

1. Make a deliberate container-compatible wrapper for each collector, using
   an explicit configurable data root (`/opt/airflow/data`) rather than
   `/workspace`, and mount only the required scripts read-only.
2. Add Etsy credentials and bounded limits through scheduler environment or a
   secrets mechanism; do not expose credentials in a DAG file.
3. Add an Advice production transform that validates each new raw snapshot and
   writes the canonical production external-product schema before it may join
   the warehouse loader. Preserve its existing 196-row validation as a test,
   not a production hard-coded cardinality rule.
4. Test the wrappers with an isolated output path, then add manual, paused
   tasks/DAGs. Only after this should they participate in a production run.
5. Define an explicit orchestration policy for 01 → 02 → 03 (for example,
   datasets or a controller DAG) rather than assuming independent manual DAGs
   enforce order.

## Verification performed

- `docker compose config --quiet`: PASS
- `airflow dags list-import-errors`: PASS (`No data found`)
- `airflow dags list`: PASS (the three DAGs above)
- `airflow tasks list` for all three DAGs: PASS
- Python syntax compilation of the DAGs and Etsy/Advice/Amazon scripts: PASS
- Container mount checks: raw data paths present; `/opt/airflow/scripts` and
  `/workspace` absent in Airflow, and `/opt/spark/scripts` absent in Spark.
- No data workflows were run.
