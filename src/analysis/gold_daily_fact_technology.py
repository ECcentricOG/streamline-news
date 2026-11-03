import os

from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from utils.logger import get_logger
from pyspark.sql.functions import col, concat_ws, lower, month, to_date, when, year

logger = get_logger(__name__)

def gold_daily_fact_technology(config:AppConfig, spark:SparkSession, downlaod_date:str):
    try:
        if os.path.exists(f"{config.GOLD_FACT_TECH_DIR}/{downlaod_date}"):
            logger.info("Data is already there so skipping")
            return 

        silver_df = spark.read\
                    .parquet(f"{config.SILVER_TRANSFORM_DIR}/{downlaod_date}")

        silver_df = silver_df.withColumn(
            "search_text",
            lower(
                concat_ws("", col("title"), col("description"), col("content"))
            )
        )

        text = col("search_text")

        silver_df = silver_df.withColumn(
            "tech_category",
            when(
                text.rlike(
                    r"\bartificial intelligence|\bai\b|machine learning|"
                    r"deep learning|generative ai|large language model|chatbot\b"
                ),
                "Artificial Intelligence"
            )
            .when(
               text.rlike(
                    r"\bcybersecurity|cyber attack|data breach|hacking|"
                    r"ransomware|malware|phishing|data security\b"
                ),
                "Cybersecurity"
            )
            .when(
                text.rlike(
                    r"\bsmartphone|mobile phone|iphone|android phone|"
                    r"mobile app|tablet\b"
                ),
                "Smartphones"
            )
            .when(
                text.rlike(
                    r"\bsoftware|application|operating system|"
                    r"app development|programming|developer\b"
                ),
                "Software Applications"
            )
            .when(
                text.rlike(
                    r"\bcloud computing|cloud storage|cloud service|"
                    r"aws|azure|google cloud\b"
                ),
                "Cloud Computing"
            )
            .when(
                text.rlike(
                    r"\brobotics|robot|humanoid robot|industrial robot\b"
                ),
                "Robotics"
            )
            .when(
                text.rlike(
                    r"\bsemiconductor|chip manufacturing|microchip|"
                    r"processor|gpu|cpu\b"
                ),
                "Semiconductors"
            )
            .when(
                text.rlike(
                    r"\b5g|6g|telecommunications|wireless network|"
                    r"broadband|internet connectivity\b"
                ), 
                "Telecom"
            )
            .when(
                text.rlike(
                    r"\blaptop|computer|smartwatch|headphones|"
                    r"consumer electronics|electronic device\b"
                ), 
                "Comsumer Electronics"
            )
            .otherwise("Non Tech")
        )

        business_df = (
            silver_df
            .filter(col("article_id").isNotNull())
            .filter(col("title").isNotNull())
            .filter(col("tech_category") != "Non Tech")
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

        business_df.write.parquet(f"{config.GOLD_FACT_TECH_DIR}/{downlaod_date}", mode="ignore")
        logger.info("Tech daily data is written")

    except Exception as e:
        spark.stop()
        logger.info(e)
