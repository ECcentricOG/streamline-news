import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

def bronze_daily_merge(spark:SparkSession, download_date:str):
    try:
        logger.info("Reading daily API data")
        api_df = spark.read.parquet(f"{os.getenv('BRONZE_API_DIR')}/{download_date}")

        api_df = api_df.select(
            "name",
            "title",
            "author",
            "description",
            "url",
            "urlToImage",
            lit(None).cast("string").alias("category"),
            "publishedAt",
            "content"
        )

        logger.info("Sucessfully readed the daily API data")

        if os.path.exists(f"{os.getenv('BRONZE_MERGE_DIR')}/{download_date}"):
            logger.info("Data is already present in the Merge table")
            return 

        logger.info("Writing daily data")

        api_df.write.mode("append")\
        .parquet(f"{os.getenv('BRONZE_MERGE_DIR')}/{download_date}")
        logger.info("Successfully written daily API data into merge datData merge is successfula")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is stopped")
        logger.info(e)
