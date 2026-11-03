import os

from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from utils.logger import get_logger
from pyspark.sql.functions import col, concat_ws, lower, month, to_date, when, year

logger = get_logger(__name__)

def gold_daily_fact_health(config:AppConfig, spark:SparkSession, download_date:str):
    try:
        if os.path.exists(f"{config.GOLD_FACT_HEALTH_DIR}/{download_date}"):
            logger.info("Data is already there so skipping")
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
            "health_category",
            when(
                text.rlike(
                    r"\bvaccine|vaccination|immunization|booster shot|"
                    r"vaccine trial\b"
                ),
                "Vaccine"
            )
            .when(
               text.rlike(
                    r"\bdisease|infection|infectious disease|virus|"
                    r"bacteria|epidemic|pandemic|outbreak|"
                    r"diabetes|cancer|heart disease\b"
                ),
                "Disease"
            )
            .when(
                text.rlike(
                    r"\bmental health|depression|anxiety|stress|"
                    r"psychological|psychiatric|mental illness\b"
                ),
                "Mental Health"
            )
            .when(
                text.rlike(
                    r"\b(nutrition|nutritional|diet|dietary|"
                    r"malnutrition|obesity|food safety)\b"
                ),
                "Nutrition"
            )
            .when(
                text.rlike(
                    r"\b(medical research|clinical trial|medical study|"
                    r"health study|medical discovery|researchers)\b"
                ),
                "Medical Research"
            )
            .when(
                text.rlike(
                    r"\b(hospital|healthcare|health care|clinic|"
                    r"healthcare system|medical facility)\b"
                ),
                "Healthcare"
            )
            .when(
                text.rlike(
                    r"\b(pharmaceutical|medicine|drug approval|"
                    r"medication|prescription drug|pharma industry)\b"
                ),
                "Pharmaceuticals"
            )
            .when(
                text.rlike(
                    r"\b(doctor|physician|nurse|surgeon|"
                    r"medical professional)\b"
                ), 
                "Doctors"
            )
            .when(
                text.rlike(
                    r"\b(diagnosis|treatment|surgery|therapy|"
                    r"medical procedure|patient care)\b"
                ), 
                "Diagnosis"
            )
            .when(
                text.rlike(
                    r"\b(fitness|wellness|physical activity|"
                    r"exercise|healthy lifestyle)\b"
                ), 
                "Wellness"
            )
            .otherwise("Non Health")
        )

        business_df = (
            silver_df
            .filter(col("article_id").isNotNull())
            .filter(col("title").isNotNull())
            .filter(col("health_category") != "Non Health")
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

        business_df.write.parquet(f"{config.GOLD_FACT_HEALTH_DIR}/{download_date}", mode="ignore")
        logger.info("Health dialy data is written")

    except Exception as e:
        spark.stop()
        logger.info(e)
