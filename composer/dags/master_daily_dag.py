import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="master_daily",
    description="Orchestrates daily Bronze, Silver and Gold processing",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["master", "daily", "etl"]
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )

    api_download = TriggerDagRunOperator(
        task_id="trigger_api_daily",
        trigger_dag_id="api_ingestion",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    bronze_daily = TriggerDagRunOperator(
        task_id="trigger_bronze_daily",
        trigger_dag_id="bronze_daily_ingest",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    silver_daily = TriggerDagRunOperator(
        task_id="trigger_silver_daily",
        trigger_dag_id="bronze_daily_transformation",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> api_download >> bronze_daily >> silver_daily >> end
