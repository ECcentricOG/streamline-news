import os

from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from utils.logger import get_logger
from schema.news_api_schema import news_schema
from pyspark.sql.functions import explode

logger = get_logger(__name__)

def bronze_ingest_api(config:AppConfig, spark:SparkSession, download_date:str):
    try:
        logger.info("Reading latest API data")
        raw_file = f"{config.RAW_API_DATA_DIR}/{download_date}.json"
        bronze_path = f"{config.BRONZE_API_DIR}/{download_date}"

        if os.path.exists(bronze_path):
            logger.info(f"Data for {download_date} already exists. Skipping.")
            return 

        news_df = spark.read.schema(news_schema).option("multiline", True).json(raw_file)

        logger.info("Getting only articles from data from all API data")

        news_article_df = news_df.select(explode("data.articles").alias("article"))

        article_df = news_article_df.select(
            "article.source.name", 
            "article.author", 
            "article.title",
            "article.description",
            "article.url",
            "article.urlToImage",
            "article.publishedAt",
            "article.content"
        )
        logger.info("Sucessfully read the API data")
        article_df.write.parquet(bronze_path, mode="append")
        logger.info(f"API data is successfully written {config.BRONZE_API_DIR}")


    except Exception as e:
        spark.stop()
        logger.info("Spark session is now stopped")
        logger.info(e)
