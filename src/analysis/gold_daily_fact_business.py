import os

from pyspark.sql.functions import lower, col, when, concat_ws, to_date, month, year
from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def gold_daily_fact_business(config:AppConfig, spark:SparkSession, download_date:str):
    try:
        if os.path.exists(f"{config.GOLD_FACT_BUSINESS_DIR}/{download_date}"):
            logger.info(f"Data already exists for date {download_date}")
            return 

        silver_df = spark.read\
                    .parquet(f"{config.SILVER_TRANSFORM_DIR}/{download_date}")

        silver_df = silver_df.withColumn(
            "search_text",
            lower(
                concat_ws("", col("title"), col("description"), col("content"))
            )
        )

        text = col("search_text")

        silver_df = silver_df.withColumn(
            "business_category",
            when(
                text.rlike(
                    r"\b(stock market|stock exchange|stock prices|"
                    r"nasdaq|s&p 500|dow jones|wall street|"
                    r"equities|share price)\b"
                ),
                "Stock Market"
            )
            .when(
               text.rlike(
                    r"\b(bank|banking|mortgage|federal reserve|"
                    r"interest rates|investment bank|"
                    r"financial services|asset management)\b"
                ),
                "Banking & Finance"
            )
            .when(
                text.rlike(
                    r"\b(inflation|gdp|recession|unemployment|"
                    r"consumer prices|economic growth|"
                    r"monetary policy|trade deficit)\b"
                ),
                "Economy"
            )
            .when(
                text.rlike(
                    r"\b(corporate earnings|quarterly earnings|"
                    r"earnings report|revenue growth|"
                    r"profit margin|net income)\b"
                ),
                "Corporate"
            )
            .when(
                text.rlike(
                     r"\b(merger|acquisition|takeover|buyout)\b"
                ),
                "Merger & Aquisitions"
            )
            .when(
                text.rlike(
                    r"\b(bitcoin|cryptocurrency|ethereum|"
                    r"crypto market|digital assets)\b"
                ),
                "Cryptocurrency"
            )
            .when(
                text.rlike(
                    r"\b(oil prices|crude oil|natural gas|"
                    r"commodities|opec|energy prices)\b"
                ),
                "Energy"
            )
            .when(
                text.rlike(
                    r"\b(business|company|companies|corporate|"
                    r"startup|startups|ceo|shareholders|"
                    r"supply chain|retail sales|layoffs)\b"
                ), 
                "Business"
            )
            .otherwise("Not Business")
        )

        business_df = (
            silver_df
            .filter(col("article_id").isNotNull())
            .filter(col("title").isNotNull())
            .filter(col("business_category") != "Not Business")
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

        business_df.write.parquet(f"{config.GOLD_FACT_BUSINESS_DIR}/{download_date}", mode="ignore")
        logger.info("Business daily data is written")

    except Exception as e:
        spark.stop()
        logger.info(e)
