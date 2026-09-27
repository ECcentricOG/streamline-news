import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import lit
from utils.logger import get_logger
from schema.news_dataset_schema import news_dataset_schema
from schema.news_article_schema import news_article_schema

load_dotenv()

logger = get_logger(__name__)

def merge_data(spark:SparkSession):
    try:
        logger.info("Reading database data from bronze layer")
        db_df = spark.read.schema(news_dataset_schema)\
            .parquet(f"{os.getenv('BRONZE_DATASET_DIR')}")
        logger.info("Successfully read the database data from bronze layer")

        logger.info("Reading API data from bronze layer")
        api_df = spark.read.schema(news_article_schema)\
            .option("recursiveFileLookup", "true")\
            .parquet(f"{os.getenv('BRONZE_API_DIR')}")
        logger.info("Successfully read the API data from bronze layer")

        logger.info("Enriching data by adding column in API dataset")
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

        logger.info("Enriching data by adding column in database dataset")
        db_df = db_df.select(
            lit(None).cast("string").alias("name"),
            "headline",
            "authors",
            "short_description",
            "link",
            lit(None).cast("string").alias("urlToImage"),
            "category",
            "date",
            lit(None).cast("string").alias("content")
        ).withColumnRenamed("headline", "title")\
        .withColumnRenamed("authors", "author")\
        .withColumnRenamed("short_description", "description")\
        .withColumnRenamed("link", "url")\
        .withColumnRenamed("date", "publishedAt")

        logger.info("Merging data of API dataset and database dataset")
        merge_df = db_df.unionByName(api_df)
        logger.info("Merge is Successful")
        merge_df.write.parquet(f"{os.getenv('BRONZE_MERGE_DIR')}/main", mode="ignore")
        logger.info("Merge data is written into bronze layer")
    except Exception as e:
        spark.stop()
        logger.info("Spark session is stopped")
        logger.info(e)

