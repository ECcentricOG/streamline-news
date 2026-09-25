from dotenv import load_dotenv
from transformations.merge_data import merge_data
from utils.logger import get_logger
from utils.spark_session import get_session

load_dotenv()

spark = get_session(__name__)
logger = get_logger(__name__)

merge_data(spark)
