import os

from dotenv import load_dotenv
from schema.news_article_schema import news_article_schema
from utils.logger import get_logger
from utils.spark_session import get_session

load_dotenv()

spark = get_session(__name__)
logger = get_logger(__name__)

read_df = spark.read.schema(news_article_schema).parquet(str(os.getenv("BRONZE_API_DIR")))
logger.info(read_df.count())
