import os

from logging import exception
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
        db_df = spark.read.schema(news_dataset_schema)\
            .parquet(f"{os.getenv('BRONZE_DATASET_DIR')}")

        api_df = spark.read.schema(news_article_schema)\
            .option("recursiveFileLookup", "true")\
            .parquet(f"{os.getenv('BRONZE_API_DIR')}")

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

        merge_df = db_df.unionByName(api_df)
        merge_df.write.parquet(f"{os.getenv('BRONZE_MERGE_DIR')}/main", mode="ignore")
    except Exception as e:
        logger.info(e)
        spark.stop()
