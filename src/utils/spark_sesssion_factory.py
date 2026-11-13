from pyspark.sql import SparkSession
from src.utils.app_config import AppConfig

class SparkSessionFactory:

    @staticmethod
    def create(app_name):
        AppConfig.validate_execution_mode()

        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
        )

        if AppConfig.SPARK_EXECUTION_MODE == "local":
            builder = (
                builder
                .master("local[*]")
                .config(
                    "spark.jars.packages",
                    "org.postgresql:postgresql:42.7.3"
                )
            )

        return builder.getOrCreate()
