from pipeline.pipeline_runner import PipelineRunner
from utils.app_config import AppConfig
from utils.spark_sesssion_factory import SparkSessionFactory
from database.db_client import DatabaseClient
from api.api_client import APIClient
from pipeline.bronze_pipeline import BronzePipeline
from pipeline.silver_pipeline import SilverPipeline
from pipeline.gold_pipeline import GoldPipeline

download_date = "2026-11-05"
config = AppConfig()
spark = SparkSessionFactory.create("Streamline_News")
api = APIClient(config, download_date)
database = DatabaseClient(config)
bronze = BronzePipeline(config, spark, download_date)
silver = SilverPipeline(config, spark, download_date)
gold = GoldPipeline(config, spark, download_date)

pipeline = PipelineRunner(api, database, bronze, silver, gold)

pipeline.run_daily()
