import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from utils.logger import get_logger
from pyspark.sql.functions import col, concat_ws, lower, month, to_date, when, year

load_dotenv()

logger = get_logger(__name__)

def gold_fact_sport(spark:SparkSession):
    try:
        silver_df = spark.read\
                    .option("recursiveFileLookup", True)\
                    .parquet(f"{os.getenv('SILVER_TRANSFORM_DIR')}")

        silver_df = silver_df.withColumn(
            "search_text",
            lower(
                concat_ws("", col("title"), col("description"), col("content"))
            )
        )

        text = col("search_text")

        silver_df = silver_df.withColumn(
            "sport_name",
            when(
                text.rlike(
                    r"\bcricket|\bipl\b|test match|odi cricket|"
                    r"t20|icc|wicket|batsman|bowler\b"
                ),
                "Cricket"
            )
            .when(
               text.rlike(
                    r"\bfootball|soccer|fifa|premier league|"
                    r"champions league|la liga|bundesliga|"
                    r"world cup\b"
                ),
                "Football"
            )
            .when(
                text.rlike(
                    r"\bbasketball|\bnba\b|wnba|basketball league\b"
                ),
                "Baseketball"
            )
            .when(
                text.rlike(
                    r"\btennis|wimbledon|us open|french open|"
                    r"australian open|atp tour|wta tour\b"
                ),
                "Tennis"
            )
            .when(
                text.rlike(
                    r"badminton|bwf|shuttlecock"
                ),
                "Badminton"
            )
            .when(
                text.rlike(
                    r"hockey|field hockey|ice hockey"
                ),
                "Hockey"
            )
            .when(
                text.rlike(
                    r"golf|pga tour|masters tournament"
                ),
                "Golf"
            )
            .when(
                text.rlike(
                    r"olympics|olympic games|paralympics"
                ), 
                "Olympics"
            )
            .otherwise("Non Sport")
        )

        business_df = (
            silver_df
            .filter(col("article_id").isNotNull())
            .filter(col("title").isNotNull())
            .filter(col("sport_name") != "Non Sport")
            .drop_duplicates(["article_id"])
        )

        business_df = (
            business_df
            .withColumn(
                "published_date",
                to_date("publishedAt")
            )
            .withColumn(
                "published_year",
                year(col("published_date"))
            )
            .withColumn(
                "published_month",
                month(col("published_date"))
            )
        )

        business_df = business_df.drop("search_text")

        columns = ["article_id"] + [
            c for c in business_df.columns
            if c != "article_id"
        ]

        business_df = business_df.select(*columns)

        business_df.write.parquet(f"{os.getenv('GOLD_FACT_SPORT_DIR')}/main", mode="ignore")

    except Exception as e:
        spark.stop()
        logger.info(e)
