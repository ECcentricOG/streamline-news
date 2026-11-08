import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from composer.utils.ingestion_database import database_ingestion

default_args = {
    "owner": "eccentric",
    "retries": 0,
}

with DAG(
    dag_id="database_ingestion",
    description="Load historical dataset into postgresql",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["database", "ingestion", "historical"],
):
    start = EmptyOperator(
        task_id="start"
    )

    load_dataset = PythonOperator(
        task_id="load_dataset_postgres",
        python_callable=database_ingestion,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> load_dataset >> end
