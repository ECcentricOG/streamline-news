from utils.app_config import AppConfig
from utils.spark_sesssion_factory import SparkSessionFactory
from utils.logger import get_logger
#Database
from database.db_client import DatabaseClient
#API
from api.api_client import APIClient
#Ingestion
from pipeline.bronze_pipeline import BronzePipeline
#Transformation
from pipeline.silver_pipeline import SilverPipeline
#Aanlysis
from pipeline.gold_pipeline import GoldPipeline

download_date = "2026-11-05"
config = AppConfig()
spark = SparkSessionFactory.create("Streamline_News")
logger = get_logger(__name__)
api = APIClient(config, download_date)
database = DatabaseClient(config)
bronze = BronzePipeline(config, spark, download_date)
silver = SilverPipeline(config, spark, download_date)
gold = GoldPipeline(config, spark, download_date)

#---------- Database ----------
#database.get_connection()
#database.insert()
#---------- API ----------
api.get_news()
api.fetch_todays_news()
#---------- Ingestion ----------
#bronze.broze_ingest_database()
bronze.bronze_ingest_api()
#bronze.merge_data()
bronze.bronze_daily_merge()
#---------- Transformation -----------
#silver.silver_ingest_data()
silver.silver_daily_ingest()
#silver.silver_transformation()
silver.silver_daily_transformation()
#---------- Aanlysis ----------
#gold.gold_fact_business()
gold.gold_daily_fact_business()
#gold.gold_fact_technology()
gold.gold_daily_fact_technology()
#gold.gold_fact_sport()
gold.gold_daily_fact_sport()
#gold.gold_fact_health()
gold.gold_daily_fact_health()
