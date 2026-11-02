from pyspark.sql import SparkSession


class SparkSessionFactory:

    @staticmethod
    def create(app_name):
        return (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.adaptive.enable", "true")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate()
        )
