import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_silver_daily_ingestion, check_silver_daily_transformation
from composer.utils.silver_transformations import silver_daily_ingestion, silver_daily_transformation

with DAG(
    dag_id="silver_daily_transformation",
    description="Daily silver layer processing",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule="0 9 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["silver", "transformation"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_daily_ingestion = BranchPythonOperator(
        task_id="check_silver_daily_ingestion",
        python_callable=check_silver_daily_ingestion
    )

    daily_ingestion = PythonOperator(
        task_id="silver_daily_ingestion",
        python_callable=silver_daily_ingestion
    )

    check_daily_transformation = BranchPythonOperator(
        task_id="check_silver_daily_transformation",
        python_callable=check_silver_daily_transformation
    )

    daily_transformation = PythonOperator(
        task_id="silver_daily_transformation",
        python_callable=silver_daily_transformation
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_daily_ingestion

    check_daily_ingestion >> [daily_ingestion, check_daily_transformation]
    daily_ingestion >> check_daily_transformation

    check_daily_transformation >> [daily_transformation, end]
    daily_transformation >> end
