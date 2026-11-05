import os

from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from utils.logger import get_logger
from pyspark.sql.functions import explode, lit
from schema.news_api_schema import news_schema
from schema.news_dataset_schema import news_dataset_schema
from schema.news_article_schema import news_article_schema

logger = get_logger(__name__)

class BronzePipeline:
    def __init__(self, config:AppConfig, spark:SparkSession, date:str, table_name:str="news_dataset") -> None:
        self.config = config
        self.spark = spark
        self.date = date
        self.table_name = table_name

    def broze_ingest_database(self):
        try:
            logger.info("Reading the data from database")
            news_dataset_df = self.spark.read.format("jdbc").options(
                url=f"jdbc:postgresql://{self.config.DB_HOST}:{self.config.DB_PORT}/{self.config.DB_NAME}",
                dbtable=self.table_name,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                driver="org.postgresql.Driver"
            ).schema(news_dataset_schema).load()
    
            logger.info("Successfully read the API data")
    
            logger.info(f"Started to writing data to {self.config.BRONZE_DATASET_DIR}")
            news_dataset_df.write.parquet(path=str(self.config.BRONZE_DATASET_DIR),mode="overwrite")
            logger.info(f"Sucessfully written data to {self.config.BRONZE_DATASET_DIR}")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is now stoped")
            logger.info(e)
    

    def bronze_ingest_api(self):
        try:
            logger.info("Reading latest API data")
            raw_file = f"{self.config.RAW_API_DATA_DIR}/{self.date}.json"
            bronze_path = f"{self.config.BRONZE_API_DIR}/{self.date}"
    
            if os.path.exists(bronze_path):
                logger.info(f"Data for {self.date} already exists. Skipping.")
                return 
    
            news_df = self.spark.read.schema(news_schema).option("multiline", True).json(raw_file)
    
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
            logger.info(f"API data is successfully written {self.config.BRONZE_API_DIR}")
    
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is now stopped")
            logger.info(e)


    def merge_data(self):
        try:
            logger.info("Reading database data from bronze layer")
            db_df = self.spark.read.schema(news_dataset_schema)\
                .parquet(f"{self.config.BRONZE_DATASET_DIR}")
            logger.info("Successfully read the database data from bronze layer")
    
            logger.info("Reading API data from bronze layer")
            api_df = self.spark.read.schema(news_article_schema)\
                .option("recursiveFileLookup", "true")\
                .parquet(f"{self.config.BRONZE_API_DIR}")
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
            merge_df.write.parquet(f"{self.config.BRONZE_MERGE_DIR}/main", mode="ignore")
            logger.info("Merge data is written into bronze layer")
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)



    def bronze_daily_merge(self):
        try:
            logger.info("Reading daily API data")
            api_df = self.spark.read.parquet(f"{self.config.BRONZE_API_DIR}/{self.date}")
    
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
    
            if os.path.exists(f"{self.config.BRONZE_MERGE_DIR}/{self.date}"):
                logger.info("Data is already present in the Merge table")
                return 
    
            logger.info("Writing daily data")
    
            api_df.write.mode("append")\
            .parquet(f"{self.config.BRONZE_MERGE_DIR}/{self.date}")
            logger.info("Successfully written daily API data into merge datData merge is successfula")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)
