import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from composer.utils.check_file import check_gold_daily_business, check_gold_daily_health, check_gold_daily_technology, check_gold_daily_sport
from composer.utils.gold_fact import gold_daily_business, gold_daily_health, gold_daily_sport, gold_daily_technology


with DAG(
    dag_id="gold_daily",
    description="Daily Gold fact-table processing",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    schedule="0 9 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["gold", "fact", "daily"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_business = BranchPythonOperator(
        task_id="check_gold_daily_business",
        python_callable=check_gold_daily_business
    )

    business_daily = PythonOperator(
        task_id="gold_daily_business",
        python_callable=gold_daily_business
    )

    check_technology = BranchPythonOperator(
        task_id="check_gold_daily_technology",
        python_callable=check_gold_daily_technology
    )

    technology_daily = PythonOperator(
        task_id="gold_daily_technology",
        python_callable=gold_daily_technology
    )

    check_sport = BranchPythonOperator(
        task_id="check_gold_daily_sport",
        python_callable=check_gold_daily_sport
    )

    sport_daily = PythonOperator(
        task_id="gold_daily_sport",
        python_callable=gold_daily_sport
    )

    check_health = BranchPythonOperator(
        task_id="check_gold_daily_health",
        python_callable=check_gold_daily_health
    )

    health_daily = PythonOperator(
        task_id="gold_daily_health",
        python_callable=gold_daily_health
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_business

    check_business >> [business_daily, check_technology]
    business_daily >> check_technology

    check_technology >> [technology_daily, check_sport]
    technology_daily >> check_sport

    check_sport >> [sport_daily, check_health]
    sport_daily >> check_health

    check_health >> [health_daily, end]
    health_daily >> end
