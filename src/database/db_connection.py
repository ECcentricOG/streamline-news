import os
import psycopg2
from dotenv import load_dotenv

from utils.logger import get_logger

load_dotenv()

def get_connection():
    logger = get_logger(__name__, "database")
    logger.info("Connecting to Database...")
    return psycopg2.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        dbname = os.getenv("DB_NAME"),
        port = os.getenv("DB_PORT")
    )
