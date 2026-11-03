import os
import psycopg2
from dotenv import load_dotenv

from utils.app_config import AppConfig
from utils.logger import get_logger

load_dotenv()

def get_connection(config: AppConfig):
    logger = get_logger(__name__, "database")
    logger.info("Connecting to Database...")
    return psycopg2.connect(
        host = config.DB_HOST,
        user = config.DB_USER,
        password = config.DB_PASSWORD,
        dbname = config.DB_NAME,
        port = config.DB_PORT
    )
