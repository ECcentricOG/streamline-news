from composer.utils.processing_date import get_processing_date
from src.utils.app_config import AppConfig
from src.pipeline.bronze_pipeline import BronzePipeline
from src.utils.spark_sesssion_factory import SparkSessionFactory


def bronze_api_ingestion(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline_news")
    processing_date = get_processing_date(**context)
    bronze = BronzePipeline(config, spark, processing_date)
    bronze.bronze_ingest_api()

def bronze_database_ingestion(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline_news")
    processing_date = get_processing_date(**context)
    bronze = BronzePipeline(config, spark, processing_date)
    bronze.broze_ingest_database()

def bronze_merge_data(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline_news")
    processing_date = get_processing_date(**context)
    bronze = BronzePipeline(config, spark, processing_date)
    bronze.merge_data()

def bronze_daily_merge(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline_news")
    processing_date = get_processing_date(**context)
    bronze = BronzePipeline(config, spark, processing_date)
    bronze.bronze_daily_merge()
