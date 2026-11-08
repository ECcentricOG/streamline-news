import pendulum

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_raw_api_file
from composer.utils.download_api_data import download_api_data

with DAG(
    dag_id="api_ingestion",
    description="Daily news API ingestion",
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
        task_id="skipping_api_download"
    )

    validate = PythonOperator(
        task_id="validate_raw_json",
        python_callable=download_api_data,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_file

    check_file >> [download, skip_download]

    [download, skip_download] >> validate

    validate >> end
