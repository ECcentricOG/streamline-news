import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_gold_daily_business, check_gold_fact_health, check_gold_fact_sport, check_gold_fact_technology
from composer.utils.gold_fact import gold_fact_business, gold_fact_health, gold_fact_sport, gold_fact_technology


with DAG (
    dag_id="gold_init",
    description="Initialization of Gold fact-tables",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["gold", "facts"]
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )

    check_business = BranchPythonOperator(
        task_id="check_gold_fact_business",
        python_callable=check_gold_daily_business
    )

    fact_business = PythonOperator(
        task_id="gold_fact_business",
        python_callable=gold_fact_business
    )

    check_technology = BranchPythonOperator(
        task_id="check_gold_fact_technology",
        python_callable=check_gold_fact_technology
    )

    fact_technology = PythonOperator(
        task_id="gold_fact_technology",
        python_callable=gold_fact_technology
    )

    check_sport = BranchPythonOperator(
        task_id="check_gold_fact_sport",
        python_callable=check_gold_fact_sport
    )
    
    fact_sport = PythonOperator(
        task_id="gold_fact_sport",
        python_callable=gold_fact_sport
    )

    check_health = BranchPythonOperator(
        task_id="check_gold_fact_health",
        python_callable=check_gold_fact_health
    )

    fact_health = PythonOperator(
        task_id="gold_fact_health",
        python_callable=gold_fact_health
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_business

    check_business >> [fact_business, check_technology]
    fact_business >> check_technology

    check_technology >> [fact_technology, check_sport]
    fact_technology >> check_sport

    check_sport >> [fact_sport, check_health]
    fact_sport >> check_health

    check_health >> [fact_health, end]
    fact_health >> end
