from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
import pendulum


with DAG(
    dag_id="mater_initialization",
    description="Orchestrates initial data pipeline setup",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["master", "initialization"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    db_init = TriggerDagRunOperator(
        task_id="trigger_database_initialization",
        trigger_dag_id="database_ingestion",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    bronze_init = TriggerDagRunOperator(
        task_id="trigger_bronze_initialization",
        trigger_dag_id="bronze_init",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    silver_init = TriggerDagRunOperator(
        task_id="trigger_silver_initialization",
        trigger_dag_id="silver_init",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    gold_init = TriggerDagRunOperator(
        task_id="trigger_gold_initialization",
        trigger_dag_id="gold_init",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> db_init >> bronze_init >> silver_init >> gold_init >> end


