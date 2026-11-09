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
        return "check_database_ingestion"
    return "bronze_api_ingestion"

def check_bronze_database_ingestion(**context):
    config = AppConfig()

    if os.path.exists(f"{config.BRONZE_DATASET_DIR}"):
        return "check_data_merge"
    return "bronze_database_ingestion"

def check_bronze_merge_data(**context):
    config = AppConfig()

    if os.path.exists(f"{config.BRONZE_MERGE_DIR}/main"):
        return "check_daily_merge"
    return "bronze_merge_data"

def check_bronze_daily_merge(**context):
    config = AppConfig()

    processing_date = get_processing_date(**context)

    if os.path.exists(f"{config.BRONZE_MERGE_DIR}/{processing_date}"):
        return "end"
    return "bronze_daily_merge"
