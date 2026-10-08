import os

from pyspark.sql import SparkSession
from schema.news_bronze_schema import news_bronze_schema
from utils.logger import get_logger
from dotenv import load_dotenv

load_dotenv()

logger = get_logger(__name__)

def silver_daily_ingest(spark:SparkSession, download_date:str):
    try:
        news_df = spark.read\
            .schema(news_bronze_schema)\
            .parquet(f"{os.getenv('BRONZE_MERGE_DIR')}/{download_date}")

        if os.path.exists(f"{os.getenv('SIVLER_INGEST_DIR')}/{download_date}"):
            logger.info("Data is already there so skipping")
            return 

        news_df = news_df.drop_duplicates()
        news_df = news_df.fillna("Unknown")

        news_df.write.parquet(f"{os.getenv("SILVER_INGEST_DIR")}/{download_date}", mode="ignore")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is stopped")
        logger.info(e)
