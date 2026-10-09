
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import conv, expr, sha2, substring


def get_unique_id(df:DataFrame):
    return df.withColumn("article_id", conv(
        substring(sha2(expr("uuid()"), 256), 1, 8),
            16, 10
        ).cast("long") % 900000000
        + 100000000
    )
