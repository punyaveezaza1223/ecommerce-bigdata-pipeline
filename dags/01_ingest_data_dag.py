from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="01_ingest_external_data",
    description="Ingest current snapshots for eBay, Rakuten, StockX, and Etsy",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    # Prevent two manual DAG runs from making duplicate concurrent API calls.
    max_active_runs=1,
    tags=["ecommerce", "ingestion", "external"],
) as dag:

    ingest_ebay = BashOperator(
        task_id="ingest_ebay",
        bash_command="python /opt/airflow/ingestion/external/ebay_ingestion.py",
        do_xcom_push=False,
    )

    ingest_rakuten = BashOperator(
        task_id="ingest_rakuten",
        bash_command="python /opt/airflow/ingestion/external/rakuten_ingestion.py",
        do_xcom_push=False,
    )

    # No dependency is declared: each marketplace can ingest independently.
    ingest_stockx = BashOperator(
        task_id="ingest_stockx",
        bash_command="python /opt/airflow/ingestion/external/stockx_ingestion.py",
        do_xcom_push=False,
    )

    # The collector's credentials come from container environment variables;
    # they are never stored in this DAG.  Limits preserve the source script's
    # bounded, checkpointed collection behaviour.
    ingest_etsy = BashOperator(
        task_id="ingest_etsy",
        bash_command=(
            "python /opt/airflow/scripts/collect_etsy_bulk.py "
            "--root \"${PIPELINE_DATA_ROOT:?PIPELINE_DATA_ROOT is required}\" "
            "--max-requests 10 --max-new-products 1000"
        ),
        do_xcom_push=False,
    )
