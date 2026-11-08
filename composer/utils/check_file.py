import os

from composer.utils.processing_date import get_processing_date
from src.utils.app_config import AppConfig

def check_raw_api_file(**context):
    config = AppConfig()

    processing_date = get_processing_date(**context)

    if os.path.exists(f"{config.RAW_API_DATA_DIR}/{processing_date}.json"):
        return "skip_api_download"
    return "download_api_news"
