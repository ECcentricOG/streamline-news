import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_bronze_database_ingestion, check_bronze_merge_data
from composer.utils.bronze_ingestion import bronze_database_ingestion, bronze_merge_data

with DAG(
    dag_id="bronze_init",
    description="Bronze layer one time database processing",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "etl", "init"]
):
    start = EmptyOperator(
        task_id = "start"
    )

    check_database_ingest = BranchPythonOperator(
        task_id="check_database_ingestion",
        python_callable=check_bronze_database_ingestion
    )

    database_ingest = PythonOperator(
        task_id="bronze_database_ingestion",
        python_callable=bronze_database_ingestion
    )

    check_merge = BranchPythonOperator(
        task_id="check_data_merge",
        python_callable=check_bronze_merge_data
    )

    merge_data = PythonOperator(
        task_id="bronze_merge_data",
        python_callable=bronze_merge_data
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_database_ingest
    
    check_database_ingest >> [database_ingest, check_merge]
    database_ingest >> check_merge

    check_merge >> [merge_data, end]
    merge_data >> end

