import os

from dotenv import load_dotenv
from utils.spark_session import get_session
from utils.logger import get_logger
from schema.news_api_schema import news_schema
from pyspark.sql.functions import explode

load_dotenv()

logger = get_logger(__name__)

def bronze_ingest_api(download_date:str):
    spark = get_session(__name__)
    try:
        logger.info("Reading latest API data")
        raw_file = f"{os.getenv('RAW_API_DATA_DIR')}/{download_date}.json"
        bronze_path = f"{os.getenv('BRONZE_API_DIR')}/{download_date}"

        if os.path.exists(bronze_path):
            logger.info(f"Data for {download_date} already exists. Skipping.")
            return 

        news_df = spark.read.schema(news_schema).option("multiline", True).json(raw_file)
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
        logger.info(f"API data is successfully written {os.getenv('BRONZE_API_DIR')}")


    except Exception as e:
        logger.info(e)
    finally:
        spark.stop()
        logger.info("Spark session is now stopped")
