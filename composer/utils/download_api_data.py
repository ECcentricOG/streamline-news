from composer.utils.processing_date import get_processing_date
from src.api.api_client import APIClient
from src.utils.app_config import AppConfig


def download_api_data(**context):
    config = AppConfig()
    processing_date = get_processing_date(**context)
    api = APIClient(config, processing_date)
    api.get_news()
    api.fetch_todays_news()
