from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="01_ingest_external_data",
    description="Ingest eBay and Rakuten data into Raw Data Lake",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "ingestion", "external"],
) as dag:

    ingest_ebay = BashOperator(
        task_id="ingest_ebay",
        bash_command="python /opt/airflow/ingestion/external/ebay_ingestion.py",
    )

    ingest_rakuten = BashOperator(
        task_id="ingest_rakuten",
        bash_command="python /opt/airflow/ingestion/external/rakuten_ingestion.py",
    )