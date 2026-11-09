import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_bronze_api_ingestion, check_bronze_daily_merge
from composer.utils.bronze_ingestion import bronze_api_ingestion, bronze_daily_merge

with DAG(
    dag_id="bronze_daily_ingest",
    description="Bronze layer daily processing",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "etl"]
):
    start = EmptyOperator(
        task_id = "start"
    )

    check_api_ingest = BranchPythonOperator(
        task_id="bronze_check_api_ingest",
        python_callable=check_bronze_api_ingestion
    )

    bronze_api_ingest = PythonOperator(
        task_id="bronze_api_ingestion",
        python_callable=bronze_api_ingestion
    )

    check_daily_merge = BranchPythonOperator(
        task_id="check_daily_merge",
        python_callable=check_bronze_daily_merge
    )

    daily_merge = PythonOperator(
        task_id="bronze_daily_merge",
        python_callable=bronze_daily_merge
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_api_ingest

    check_api_ingest >> [bronze_api_ingest, check_daily_merge]
    bronze_api_ingest >> check_daily_merge

    check_daily_merge >> [daily_merge, end]
    daily_merge >> end
