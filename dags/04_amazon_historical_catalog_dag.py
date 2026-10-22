"""Isolated, read-only preflight for the Amazon 2023 historical catalog.

This DAG deliberately has no edge to the current price-snapshot ETL or
fact_external_product loader.  The existing catalog import remains an explicit
approval-gated operation because it is a large, dimension-only backfill.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="04_amazon_historical_catalog",
    description="Validate Amazon 2023 historical catalog source; no price facts",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "amazon", "historical-catalog", "manual-only"],
) as dag:
    validate_historical_catalog_source = BashOperator(
        task_id="validate_historical_catalog_source",
        bash_command=(
            "python /opt/airflow/scripts/validate_amazon_historical_catalog.py "
            "--root \"${PIPELINE_DATA_ROOT:?PIPELINE_DATA_ROOT is required}\""
        ),
        do_xcom_push=False,
    )
