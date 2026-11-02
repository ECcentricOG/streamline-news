import os
from dotenv import load_dotenv

load_dotenv()

class AppConfig:
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    
    API_KEY = os.getenv('API_KEY')
    
    RAW_DATASET_FILE_PATH = os.getenv('RAW_DATASET_FILE_PATH')
    RAW_API_DATA_DIR = os.getenv('RAW_API_DATA_DIR')
    BRONZE_DATASET_DIR = os.getenv('BRONZE_DATASET_DIR')
    BRONZE_API_DIR = os.getenv('BRONZE_API_DIR')
    BRONZE_MERGE_DIR = os.getenv('BRONZE_MERGE_DIR')
    SILVER_INGEST_DIR = os.getenv('SILVER_INGEST_DIR')
    SILVER_TRANSFORM_DIR = os.getenv('SILVER_TRANSFORM_DIR')
    GOLD_FACT_BUSINESS_DIR = os.getenv('GOLD_FACT_BUSINESS_DIR')
    GOLD_FACT_TECH_DIR = os.getenv('GOLD_FACT_TECH_DIR')
    GOLD_FACT_SPORT_DIR = os.getenv('GOLD_FACT_SPORT_DIR')
    GOLD_FACT_HEALTH_DIR = os.getenv('GOLD_FACT_HEALTH_DIR')
