from pyspark.sql.types import StringType, StructField, StructType


news_schema = StructType([
    StructField("name", StringType(), True),
    StructField("authors", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True),
])
