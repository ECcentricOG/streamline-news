from pyspark.sql import SparkSession
from schema.news_dataset_schema import news_dataset_schema
from utils.app_config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def broze_ingest_database(config:AppConfig, spark:SparkSession, table_name:str="news_dataset"):
    try:
        logger.info("Reading the data from database")
        news_dataset_df = spark.read.format("jdbc").options(
            url=f"jdbc:postgresql://{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}",
            dbtable=table_name,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            driver="org.postgresql.Driver"
        ).schema(news_dataset_schema).load()

        logger.info("Successfully read the API data")

        logger.info(f"Started to writing data to {config.BRONZE_DATASET_DIR}")
        news_dataset_df.write.parquet(path=str(config.BRONZE_DATASET_DIR),mode="overwrite")
        logger.info(f"Sucessfully written data to {config.BRONZE_DATASET_DIR}")

    except Exception as e:
        spark.stop()
        logger.info("Spark session is now stoped")
        logger.info(e)
