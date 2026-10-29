from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

news_silver_schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("author", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("urlToImage", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("publishedAt", DateType(), nullable=True),
    StructField("content", StringType(), nullable=True),
    StructField("article_id", IntegerType(), nullable=False)
])
