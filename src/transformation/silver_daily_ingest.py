import os

from pyspark.sql import SparkSession
from schema.news_bronze_schema import news_bronze_schema
from utils.app_config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def silver_daily_ingest(config:AppConfig, spark:SparkSession, download_date:str):
    try:
        news_df = spark.read\
            .schema(news_bronze_schema)\
            .parquet(f"{config.BRONZE_MERGE_DIR}/{download_date}")

        if os.path.exists(f"{config.SILVER_INGEST_DIR}/{download_date}"):
            logger.info("Data is already there so skipping")
            return 

        news_df = news_df.drop_duplicates()
        news_df = news_df.fillna("Unknown")

        news_df.write.parquet(f"{config.SILVER_INGEST_DIR}/{download_date}", mode="ignore")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is stopped")
        logger.info(e)
