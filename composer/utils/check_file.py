import os

from composer.utils.processing_date import get_processing_date
from src.utils.app_config import AppConfig

def check_raw_api_file(**context):
    config = AppConfig()

    processing_date = get_processing_date(**context)

    if os.path.exists(f"{config.RAW_API_DATA_DIR}/{processing_date}.json"):
        return "skip_api_download"
    return "download_api_news"

def check_bronze_api_ingestion(**context):
    config = AppConfig()
    processing_date = get_processing_date(**context)
    if os.path.exists(f"{config.BRONZE_API_DIR}/{processing_date}"):
        return "check_daily_merge"
    return "bronze_api_ingestion"

def check_bronze_daily_merge(**context):
    config = AppConfig()
    processing_date = get_processing_date(**context)
    if os.path.exists(f"{config.BRONZE_MERGE_DIR}/{processing_date}"):
        return "end"
    return "bronze_daily_merge"

def check_bronze_database_ingestion(**context):
    config = AppConfig()
    if os.path.exists(f"{config.BRONZE_DATASET_DIR}"):
        return "check_data_merge"
    return "bronze_database_ingestion"

def check_bronze_merge_data(**context):
    config = AppConfig()
    if os.path.exists(f"{config.BRONZE_MERGE_DIR}/main"):
        return "end"
    return "bronze_merge_data"

def check_silver_ingestion(**config):
    config = AppConfig()
    if os.path.exists(f"{config.SILVER_INGEST_DIR}/main"):
        return "check_silver_transformation"
    return "silver_ingestion"

def check_silver_transformation(**context):
    config = AppConfig()
    if os.path.exists(f"{config.SILVER_TRANSFORM_DIR}/main"):
        return "end"
    return "silver_transformation"

def check_silver_daily_ingestion(**context):
    config = AppConfig()
    processing_date = get_processing_date(**context)
    if os.path.exists(f"{config.SILVER_INGEST_DIR}/{processing_date}"):
        return "check_silver_daily_transformation"
    return "silver_daily_ingestion"

def check_silver_daily_transformation(**context):
    config = AppConfig()
    processing_date = get_processing_date(**context)
    if os.path.exists(f"{config.SILVER_TRANSFORM_DIR}/{processing_date}"):
        return "end"
    return "silver_daily_transformation"
