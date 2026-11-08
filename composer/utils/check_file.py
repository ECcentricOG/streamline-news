import os

from src.utils.app_config import AppConfig

def check_raw_api_file(config:AppConfig, processing_date:str):
    if os.path.exists(f"{config.RAW_API_DATA_DIR}/{processing_date}.json"):
        return "Skip API news"
    return "Download API news"
