from pyspark.sql import SparkSession

def get_session(name="Default"):
    session = SparkSession.builder\
            .appName(name)\
            .master("local[*]")\
            .getOrCreate()

    return session
