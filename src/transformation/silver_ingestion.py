import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

def silver_ingest_data(spark:SparkSession):
    try:
        news_df = spark.read.option("recursiveFileLookup", "true")\
            .parquet(f"{os.getenv('BRONZE_MERGE_DIR')}")

        news_df = news_df.drop_duplicates()
        news_df = news_df.fillna("Unknown")

        news_df.write.parquet(f"{os.getenv("SILVER_INGEST_DIR")}/main", mode="ignore")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is stopped")
        logger.info(e)
