from composer.utils.processing_date import get_processing_date
from src.pipeline.silver_pipeline import SilverPipeline
from src.utils.app_config import AppConfig
from src.utils.spark_sesssion_factory import SparkSessionFactory

def silver_ingestion(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    silver = SilverPipeline(config, spark, processing_date)
    silver.silver_ingest_data()

def silver_transformation(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    silver = SilverPipeline(config, spark, processing_date)
    silver.silver_transformation()

def silver_daily_ingestion(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    silver = SilverPipeline(config, spark, processing_date)
    silver.silver_daily_ingest()

def silver_daily_transformation(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    silver = SilverPipeline(config, spark, processing_date)
    silver.silver_daily_transformation()
