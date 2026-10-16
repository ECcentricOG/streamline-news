import os

from dotenv import load_dotenv
from ingestion.bronze_daily_merge import bronze_daily_merge
from ingestion.bronze_ingest_api import bronze_ingest_api
from transformation.silver_daily_transformation import silver_daily_transformation
from transformation.silver_daily_ingest import silver_daily_ingest
from utils.logger import get_logger
from utils.spark_session import get_session
from utils.read_df import read_df
from api.fetch_news import fetch_todays_news


download_date = "2026-10-15"

load_dotenv()

spark = get_session(__name__)
logger = get_logger(__name__)

#fetch_todays_news(download_date)
#bronze_ingest_api(spark, download_date)
#bronze_daily_merge(spark, download_date)
#silver_daily_ingest(spark, download_date)
#silver_daily_transformation(spark, download_date)

df = read_df(spark, f"{os.getenv('SILVER_TRANSFORM_DIR')}")
df.filter(df["name"] != "Unknown").filter(df['image'] != "Unknown").show(50)
