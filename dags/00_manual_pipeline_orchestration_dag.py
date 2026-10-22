"""Manual controller for the current-snapshot pipeline.

The source DAGs retain schedule=None and can still be run individually.  This
controller provides the explicit 01 -> 02 -> 03 order when a complete pipeline
run is intentionally requested.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState


with DAG(
    dag_id="00_manual_pipeline_orchestration",
    description="Manual-only controller: ingest -> Spark ETL -> warehouse load",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "orchestration", "manual-only"],
) as dag:
    run_ingestion = TriggerDagRunOperator(
        task_id="run_ingestion",
        trigger_dag_id="01_ingest_external_data",
        wait_for_completion=True,
        reset_dag_run=False,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_spark_etl = TriggerDagRunOperator(
        task_id="run_spark_etl",
        trigger_dag_id="02_spark_etl",
        wait_for_completion=True,
        reset_dag_run=False,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_warehouse_load = TriggerDagRunOperator(
        task_id="run_warehouse_load",
        trigger_dag_id="03_load_warehouse",
        wait_for_completion=True,
        reset_dag_run=False,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_ingestion >> run_spark_etl >> run_warehouse_load
