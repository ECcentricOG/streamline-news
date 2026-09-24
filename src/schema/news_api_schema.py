from pyspark.sql.types import ArrayType, DateType, IntegerType, StringType, StructField, StructType

source_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

article_schema = StructType([
    StructField("source", source_schema, True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", DateType(), True),
    StructField("content", StringType(), True),
])

data_schema = StructType([
    StructField("status", StringType(), True),
    StructField("totalResults", IntegerType(), True),
    StructField("articles", ArrayType(article_schema), True)
])

news_schema = StructType([
    StructField("download_date", StringType(), True),
    StructField("source", StringType(), True),
    StructField("data",data_schema ,True)
])
