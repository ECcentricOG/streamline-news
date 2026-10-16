from pyspark.sql import SparkSession


def read_df(spark:SparkSession, path:str):
    return spark.read.option("recursiveFileLookup", "true").parquet(path)
