from pyspark.sql.types import DateType, StringType, StructField, StructType


news_article_schema = StructType([
    StructField("name", StringType(), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", DateType(), True),
    StructField("content", StringType(), True),
])
