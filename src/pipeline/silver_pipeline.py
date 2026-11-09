import os

from pyspark.sql import SparkSession
from src.utils.app_config import AppConfig
from src.utils.logger import get_logger
from src.utils.unique_article_id import get_unique_id
from pyspark.sql.functions import lit, trim, col, when
from src.schema.news_bronze_schema import news_bronze_schema

logger = get_logger(__name__)

class SilverPipeline:

    def __init__(self, config:AppConfig, spark:SparkSession, date:str) -> None:
        self.config = config
        self.spark = spark
        self.date = date


    def silver_ingest_data(self):
        try:
            news_df = self.spark.read.option("recursiveFileLookup", "true")\
                .parquet(f"{self.config.BRONZE_MERGE_DIR}")
    
            news_df = news_df.drop_duplicates()
            news_df = news_df.fillna("Unknown")
    
            news_df.write.parquet(f"{self.config.SILVER_INGEST_DIR}/main", mode="ignore")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)


    def silver_daily_ingest(self):
        try:
            news_df = self.spark.read\
                .schema(news_bronze_schema)\
                .parquet(f"{self.config.BRONZE_MERGE_DIR}/{self.date}")
    
            if os.path.exists(f"{self.config.SILVER_INGEST_DIR}/{self.date}"):
                logger.info("Data is already there so skipping")
                return 
    
            news_df = news_df.drop_duplicates()
            news_df = news_df.fillna("Unknown")
    
            news_df.write.parquet(f"{self.config.SILVER_INGEST_DIR}/{self.date}", mode="ignore")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)


    def silver_transformation(self):
        try:
            news_df = self.spark.read.schema(news_bronze_schema)\
                        .option("recursiveFileLookup", "true")\
                        .parquet(f"{self.config.SILVER_INGEST_DIR}")
    
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
    
            news_df.write.parquet(f"{self.config.SILVER_TRANSFORM_DIR}/main", mode="ignore")
    
        except Exception as e: 
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)


    def silver_daily_transformation(self):
        try:
            news_df = self.spark.read.schema(news_bronze_schema)\
                        .parquet(f"{self.config.SILVER_INGEST_DIR}/{self.date}")
    
            if os.path.exists(f"{self.config.SILVER_TRANSFORM_DIR}/{self.date}"):
                return
    
            news_df = get_unique_id(news_df)
            news_df = news_df.withColumnRenamed("urlToImage", "image")
            news_df = news_df.drop_duplicates()
    
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
    
            news_df.write.parquet(f"{self.config.SILVER_TRANSFORM_DIR}/{self.date}", mode="ignore")
    
        except Exception as e: 
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)
