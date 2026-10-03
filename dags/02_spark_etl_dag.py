from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="02_spark_etl",
    description="Clean and transform external e-commerce data using Apache Spark",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),
    tags=["ecommerce", "spark", "etl"],
) as dag:

    spark_clean_external = BashOperator(
        task_id="spark_clean_external",
        bash_command="""
        docker exec pipeline-spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --executor-memory 768m \
        --driver-memory 768m \
        --conf spark.executor.cores=1 \
        --conf spark.sql.shuffle.partitions=4 \
        --conf spark.default.parallelism=4 \
        /opt/spark/jobs/spark_clean.py
        """,
        execution_timeout=timedelta(minutes=25),
    )