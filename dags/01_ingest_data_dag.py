from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# DAG: External Data Ingestion
# ============================================================

with DAG(
    dag_id="01_ingest_external_data",
    description="Ingest eBay, Mercado Libre and Best Buy data into Raw Data Lake",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "ingestion", "external"],
) as dag:

    # --------------------------------------------------------
    # eBay (implementation retained in the existing filename during migration)
    # --------------------------------------------------------
    ingest_ebay = BashOperator(
        task_id="ingest_ebay",
        bash_command="python /opt/airflow/ingestion/external/shopee_ingestion.py",
    )

    # --------------------------------------------------------
    # Mercado Libre (implementation retained in the existing filename during migration)
    # --------------------------------------------------------
    ingest_mercado_libre = BashOperator(
        task_id="ingest_mercado_libre",
        bash_command="python /opt/airflow/ingestion/external/lazada_ingestion.py",
    )

    # --------------------------------------------------------
    # Best Buy (implementation retained in the existing filename during migration)
    # --------------------------------------------------------
    ingest_best_buy = BashOperator(
        task_id="ingest_best_buy",
        bash_command="python /opt/airflow/ingestion/external/amazon_ingestion.py",
    )

    # ทั้ง 3 platform สามารถทำงานพร้อมกันได้
    [ingest_ebay, ingest_mercado_libre, ingest_best_buy]
