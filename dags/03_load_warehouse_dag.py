from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="03_load_warehouse",
    description="Load Spark processed data into PostgreSQL Warehouse",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=[
        "ecommerce",
        "warehouse"
    ],
) as dag:


    load_external_product = BashOperator(

        task_id="load_external_product",

        bash_command="""

        docker exec pipeline-spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 1g \
        --executor-memory 768m \
        --conf spark.executor.cores=1 \
        /opt/spark/jobs/load_warehouse.py

        """
    )