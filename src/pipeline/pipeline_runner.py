from src.api.api_client import APIClient
from src.database.db_client import DatabaseClient
from src.pipeline.bronze_pipeline import BronzePipeline
from src.pipeline.gold_pipeline import GoldPipeline
from src.pipeline.silver_pipeline import SilverPipeline


class PipelineRunner:

    def __init__(self, api:APIClient, db:DatabaseClient, bronze:BronzePipeline, silver:SilverPipeline, gold:GoldPipeline) -> None:
        self.api = api
        self.db = db
        self.bronze = bronze
        self.silver = silver
        self.gold = gold

    def run_daily(self):
        self.api.get_news()
        self.api.fetch_todays_news()
        self.bronze.bronze_ingest_api()
        self.bronze.bronze_daily_merge()
        self.silver.silver_daily_ingest()
        self.silver.silver_daily_transformation()
        self.gold.gold_daily_fact_business()
        self.gold.gold_daily_fact_technology()
        self.gold.gold_daily_fact_sport()
        self.gold.gold_daily_fact_health()

    def run_databse(self):
        self.db.get_connection()
        self.db.insert()

    def run_api(self):
        self.api.get_news()
        self.api.fetch_todays_news()

    def run_bronze(self):
        self.bronze.broze_ingest_database()
        self.bronze.bronze_ingest_api()
        self.bronze.merge_data()
        self.bronze.bronze_daily_merge()

    def run_silver(self):
        self.silver.silver_ingest_data()
        self.silver.silver_daily_ingest()
        self.silver.silver_transformation()
        self.silver.silver_daily_transformation()

    def run_gold(self):
        self.gold.gold_fact_business()
        self.gold.gold_daily_fact_business()
        self.gold.gold_fact_technology()
        self.gold.gold_daily_fact_technology()
        self.gold.gold_fact_sport()
        self.gold.gold_daily_fact_sport()
        self.gold.gold_fact_health()
        self.gold.gold_daily_fact_health()

    def run(self):
        self.run_databse()
        self.run_api()
        self.run_bronze()
        self.run_silver()
        self.run_gold()
