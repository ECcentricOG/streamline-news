import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from schema.news_dataset_schema import news_dataset_schema
from utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

def broze_ingest_database(spark:SparkSession, table_name:str):
    try:
        logger.info("Reading the data from database")
        news_dataset_df = spark.read.format("jdbc").options(
            url=f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
            dbtable=table_name,
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            driver="org.postgresql.Driver"
        ).schema(news_dataset_schema).load()

        logger.info("Successfully read the API data")

        logger.info(f"Started to writing data to {os.getenv('BRONZE_DATASET_DIR')}")
        news_dataset_df.write.parquet(path=str(os.getenv("BRONZE_DATASET_DIR")),mode="ignore")
        logger.info(f"Sucessfully written data to {os.getenv('BRONZE_DATASET_DIR')}")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is now stoped")
        logger.info(e)
