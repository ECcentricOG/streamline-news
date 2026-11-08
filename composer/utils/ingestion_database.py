from src.utils.app_config import AppConfig
from src.database.db_client import DatabaseClient


def database_ingestion(**context):
    config = AppConfig()
    db = DatabaseClient(config)
    db.get_connection()
    db.insert()
