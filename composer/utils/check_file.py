from composer.utils.processing_date import get_processing_date
from src.utils.app_config import AppConfig
from src.utils.storage_client import StorageClient

config = AppConfig()
storage = StorageClient()

def check_raw_api_file(**context):

    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.RAW_API_DATA_DIR}", f"{processing_date}.json")

    if storage.exists(file_path):
        return "skip_api_download"
    return "download_api_news"

def check_bronze_api_ingestion(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.BRONZE_API_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "check_daily_merge"
    return "bronze_api_ingestion"

def check_bronze_daily_merge(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.BRONZE_MERGE_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "end"
    return "bronze_daily_merge"

def check_bronze_database_ingestion(**context):
    if storage.exists(f"{config.BRONZE_DATASET_DIR}"):
        return "check_data_merge"
    return "bronze_database_ingestion"

def check_bronze_merge_data(**context):
    if storage.exists(f"{config.BRONZE_MERGE_DIR}/main"):
        return "end"
    return "bronze_merge_data"

def check_silver_ingestion(**context):
    if storage.exists(f"{config.SILVER_INGEST_DIR}/main"):
        return "check_silver_transformation"
    return "silver_ingestion"

def check_silver_transformation(**context):
    if storage.exists(f"{config.SILVER_TRANSFORM_DIR}/main"):
        return "end"
    return "silver_transformation"

def check_silver_daily_ingestion(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.SILVER_INGEST_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "check_silver_daily_transformation"
    return "silver_daily_ingestion"

def check_silver_daily_transformation(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.SILVER_TRANSFORM_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "end"
    return "silver_daily_transformation"

def check_gold_fact_business(**context):
    if storage.exists(f"{config.GOLD_FACT_BUSINESS_DIR}/main"):
        return "check_gold_fact_technology"
    return "gold_fact_business"

def check_gold_fact_technology(**context):
    if storage.exists(f"{config.GOLD_FACT_TECH_DIR}/main"):
        return "check_gold_fact_sport"
    return "gold_fact_sport"

def check_gold_fact_sport(**context):
    if storage.exists(f"{config.GOLD_FACT_SPORT_DIR}/main"):
        return "check_gold_fact_health"
    return "gold_fact_sport"

def check_gold_fact_health(**context):
    if storage.exists(f"{config.GOLD_FACT_HEALTH_DIR}/main"):
        return "end"
    return "gold_fact_health"

def check_gold_daily_business(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.GOLD_FACT_BUSINESS_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "check_gold_daily_technology"
    return "gold_daily_business"

def check_gold_daily_technology(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.GOLD_FACT_TECH_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "check_gold_daily_sport"
    return "gold_daily_technology"

def check_gold_daily_sport(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.GOLD_FACT_SPORT_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "check_gold_daily_health"
    return "gold_daily_sport"

def check_gold_daily_health(**context):
    processing_date = get_processing_date(**context)
    file_path = storage.join_path(f"{config.GOLD_FACT_HEALTH_DIR}", f"{processing_date}")
    if storage.exists(file_path):
        return "end"
    return "gold_daily_health"
