import os

from dotenv import load_dotenv
from transformation.silver_ingestion import silver_ingest_data
from utils.logger import get_logger
from utils.spark_session import get_session


download_date = "2026-09-27"

load_dotenv()

spark = get_session(__name__)
logger = get_logger(__name__)

merge_df = spark.read\
    .option("recursiveFileLookup", "true")\
    .parquet(f"{os.getenv('SILVER_INGEST_DIR')}")

logger.info(merge_df.count())
merge_df.show(7)
