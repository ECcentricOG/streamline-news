from pyspark.sql.types import DateType, StringType, StructField, StructType

news_bronze_schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("author", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("urlToImage", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("publishedAt", DateType(), nullable=True),
    StructField("content", StringType(), nullable=True)
])
