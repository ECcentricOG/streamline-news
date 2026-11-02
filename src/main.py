import os

from dotenv import load_dotenv
from analysis.gold_daily_fact_business import gold_daily_fact_business
from analysis.gold_daily_fact_sport import gold_daily_fact_sport
from analysis.gold_daily_fact_technology import gold_daily_fact_technology
from analysis.gold_dialy_fact_health import gold_daily_fact_health
from utils.spark_sesssion_factory import SparkSessionFactory
from utils.logger import get_logger
from utils.read_df import read_df

download_date = "2026-10-15"

load_dotenv()

spark = SparkSessionFactory.create("Streamline_News")
logger = get_logger(__name__)


gold_daily_fact_business(spark, download_date)
gold_daily_fact_technology(spark, download_date)
gold_daily_fact_sport(spark, download_date)
gold_daily_fact_health(spark, download_date)

df = read_df(spark, f"{os.getenv('GOLD_FACT_BUSINESS_DIR')}")
df.show(40)
print("BUSINESS", df.count())
df = read_df(spark, f"{os.getenv('GOLD_FACT_TECH_DIR')}")
df.show(40)
print("TECH", df.count())
df = read_df(spark, f"{os.getenv('GOLD_FACT_SPORT_DIR')}")
df.show(40)
print("SPORT", df.count())
df = read_df(spark, f"{os.getenv('GOLD_FACT_HEALTH_DIR')}")
df.show(40)
print("HEALTH", df.count())
