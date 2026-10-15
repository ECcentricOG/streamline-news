import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, trim, col, when
from dotenv import load_dotenv
from utils.logger import get_logger
from schema.news_bronze_schema import news_bronze_schema
from utils.unique_article_id import get_unique_id

load_dotenv()

logger = get_logger(__name__)

def silver_transformation(spark:SparkSession):
    try:
        news_df = spark.read.schema(news_bronze_schema)\
                    .option("recursiveFileLookup", "true")\
                    .parquet(f"{os.getenv('SILVER_INGEST_DIR')}")

        news_df = get_unique_id(news_df)
        news_df.withColumnRenamed("urlToImage", "image")
        news_df.drop_duplicates()

        string_cols = ["name", "title", "author", "url", "image", "description", "category", "content"]

        for column in string_cols:
            if column in news_df.columns:
                cleaned = trim(col(column))
                news_df = news_df.withColumn(
                    column,
                    when(
                        col(column).isNull() | (cleaned == ""),
                        lit(None).cast("string")
                    ).otherwise(cleaned)
                )

        news_df.write.parquet(f"{os.getenv('SILVER_TRANSFORM_DIR')}/main", mode="ignore")

    except Exception as e: 
        logger.info(e)
