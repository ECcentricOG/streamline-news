from src.utils.app_config import AppConfig
from src.pipeline.gold_pipeline import GoldPipeline
from composer.utils.processing_date import get_processing_date
from src.utils.spark_sesssion_factory import SparkSessionFactory

def gold_fact_business(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("steamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_fact_business()

def gold_fact_technology(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("steamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_fact_technology()

def gold_fact_sport(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("steamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_fact_sport()

def gold_fact_health(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("steamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_fact_health()

def gold_daily_business(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_daily_fact_business()

def gold_daily_technology(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_daily_fact_technology()

def gold_daily_sport(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_daily_fact_sport()

def gold_daily_health(**context):
    config = AppConfig()
    spark = SparkSessionFactory.create("streamline-news")
    processing_date = get_processing_date(**context)
    gold = GoldPipeline(config, spark, processing_date)
    gold.gold_daily_fact_health()

