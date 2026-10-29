from pyspark.sql import SparkSession

from utils.logger import get_logger

logger = get_logger(__name__)

def get_session(name="Default"):

    logger.info("Spark session is running")

    return SparkSession.builder\
        .appName(name)\
        .master("local[*]")\
        .config("spark.sql.adaptive.enable", "true")\
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")\
        .getOrCreate()
    

class SparkSessionFactory:

    @staticmethod
    def create(app_name="Default"):
        return SparkSession.builder\
            .appName(app_name)\
            .master("local[*]")\
            .config("spark.sql.adaptive.enable", "true")\
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")\
            .getOrCreate()
