import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_silver_ingestion, check_silver_transformation
from composer.utils.silver_transformations import silver_ingestion, silver_transformation

with DAG(
    dag_id="silver_init",
    description="Silver layer transformation of dataset",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transformation"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_ingestion = BranchPythonOperator(
        task_id="check_silver_ingestion",
        python_callable=check_silver_ingestion
    )

    silver_ingestion = PythonOperator(
        task_id="silver_ingestion",
        python_callable=silver_ingestion
    )

    check_transformation = BranchPythonOperator(
        task_id="check_silver_transformation",
        python_callable=check_silver_transformation
    )

    silver_transformation = PythonOperator(
        task_id="silver_transformation",
        python_callable=silver_transformation
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_ingestion

    check_ingestion >> [silver_ingestion, check_transformation]
    silver_ingestion >> check_transformation

    check_transformation >> [silver_transformation, end]
    silver_transformation >> end
