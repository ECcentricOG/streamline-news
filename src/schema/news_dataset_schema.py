from pyspark.sql.types import DateType, StringType, StructField, StructType

news_dataset_schema = StructType([
    StructField("link", StringType(), nullable=True),
    StructField("headline", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("short_description", StringType(), nullable=True),
    StructField("authors", StringType(), nullable=True),
    StructField("date", DateType(), nullable=True),
])
