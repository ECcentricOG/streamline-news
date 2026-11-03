from utils.app_config import AppConfig
from utils.spark_sesssion_factory import SparkSessionFactory
from utils.logger import get_logger

#Database
from database.db_connection import get_connection
from database.insert_into_table import insert

#API
from api.api_client import get_news
from api.fetch_news import fetch_todays_news

#Ingestion
from ingestion.bronze_ingest_database import broze_ingest_database
from ingestion.bronze_ingest_api import bronze_ingest_api
from ingestion.bronze_merge_data import merge_data
from ingestion.bronze_daily_merge import bronze_daily_merge

#Transformation
from transformation.silver_ingestion import silver_ingest_data
from transformation.silver_daily_ingest import silver_daily_ingest
from transformation.silver_transformation import silver_transformation
from transformation.silver_daily_transformation import silver_daily_transformation

#Aanlysis
from analysis.gold_fact_bussiness_news import gold_fact_business
from analysis.gold_daily_fact_business import gold_daily_fact_business
from analysis.gold_fact_technology_news import gold_fact_technology
from analysis.gold_daily_fact_technology import gold_daily_fact_technology
from analysis.gold_fact_sport_news import gold_fact_sport
from analysis.gold_daily_fact_sport import gold_daily_fact_sport
from analysis.gold_fact_health_news import gold_fact_health
from analysis.gold_dialy_fact_health import gold_daily_fact_health

download_date = "2026-11-03"
config = AppConfig()
spark = SparkSessionFactory.create("Streamline_News")
logger = get_logger(__name__)

#---------- Database ----------
#get_connection(config)
#insert(config)

#---------- API ----------
#get_news(config)
#fetch_todays_news(config, download_date)

#---------- Ingestion ----------
#broze_ingest_database(config, spark, "news_dataset")
#bronze_ingest_api(config, spark, download_date)
#merge_data(config, spark)
#bronze_daily_merge(config, spark, download_date)

#---------- Transformation -----------
#silver_ingest_data(config, spark)
#silver_daily_ingest(config, spark, download_date)
#silver_transformation(config, spark)
#silver_daily_transformation(config, spark, download_date)

#---------- Aanlysis ----------
#gold_fact_business(config, spark)
gold_daily_fact_business(config, spark, download_date)
#gold_fact_health(config, spark)
gold_daily_fact_health(config, spark, download_date)
#gold_fact_sport(config, spark)
gold_daily_fact_sport(config, spark, download_date)
#gold_fact_technology(config, spark)
gold_daily_fact_technology(config, spark, download_date)
