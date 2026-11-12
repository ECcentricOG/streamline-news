import pendulum

from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_raw_api_file
from composer.utils.download_api_data import download_api_data

default_args = {
    "owner": "eccentric",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_ingestion",
    description="Daily news API ingestion",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule="0 9 * * *",
    catchup=False,
    tags=["ingestion", "api", "raw"]
):
    start = EmptyOperator(
        task_id="start"
    )

    check_file = BranchPythonOperator(
        task_id="check_existing_raw_file",
        python_callable=check_raw_api_file
    )

    download = PythonOperator(
        task_id="download_api_news",
        python_callable=download_api_data
    )

    skip_download = EmptyOperator(
        task_id="skip_api_download"
    )

    validate = PythonOperator(
        task_id="validate_raw_json",
        python_callable=download_api_data
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_file

    check_file >> [download, skip_download]

    [download, skip_download] >> validate

    validate >> end
