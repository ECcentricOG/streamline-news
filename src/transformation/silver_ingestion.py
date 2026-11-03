from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def silver_ingest_data(config:AppConfig, spark:SparkSession):
    try:
        news_df = spark.read.option("recursiveFileLookup", "true")\
            .parquet(f"{config.BRONZE_MERGE_DIR}")

        news_df = news_df.drop_duplicates()
        news_df = news_df.fillna("Unknown")

        news_df.write.parquet(f"{config.SILVER_INGEST_DIR}/main", mode="ignore")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is stopped")
        logger.info(e)
