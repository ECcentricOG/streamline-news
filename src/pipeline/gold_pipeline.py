import os

from pyspark.sql import SparkSession
from utils.app_config import AppConfig
from pyspark.sql.functions import col, concat_ws, lower, month, to_date, when, year
from utils.logger import get_logger

logger = get_logger(__name__)

class GoldPipeline:

    def __init__(self, config:AppConfig, spark:SparkSession, date:str) -> None:
        self.config = config
        self.spark = spark
        self.date = date


    def gold_fact_business(self):
        try:
            silver_df = self.spark.read\
                        .option("recursiveFileLookup", True)\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_BUSINESS_DIR}/main", mode="ignore")
            logger.info("Business data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)


    def gold_daily_fact_business(self):
        try:
            if os.path.exists(f"{self.config.GOLD_FACT_BUSINESS_DIR}/{self.date}"):
                logger.info(f"Data already exists for date {self.date}")
                return 
    
            silver_df = self.spark.read\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}/{self.date}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_BUSINESS_DIR}/{self.date}", mode="ignore")
            logger.info("Business daily data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)




    def gold_fact_technology(self):
        try:
            silver_df = self.spark.read\
                        .option("recursiveFileLookup", True)\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_TECH_DIR}/main", mode="ignore")
            logger.info("Tech data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)



    def gold_daily_fact_technology(self):
        try:
            if os.path.exists(f"{self.config.GOLD_FACT_TECH_DIR}/{self.date}"):
                logger.info("Data is already there so skipping")
                return 
    
            silver_df = self.spark.read\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}/{self.date}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_TECH_DIR}/{self.date}", mode="ignore")
            logger.info("Tech daily data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)



    def gold_fact_sport(self):
        try:
            silver_df = self.spark.read\
                        .option("recursiveFileLookup", True)\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_SPORT_DIR}/main", mode="ignore")
            logger.info("Sports data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)



    def gold_daily_fact_sport(self):
        try:
            if os.path.exists(f"{self.config.GOLD_FACT_SPORT_DIR}/{self.date}"):
                logger.info("Data is alredy there skipping")
                return 
    
            silver_df = self.spark.read\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}/{self.date}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_SPORT_DIR}/{self.date}", mode="ignore")
            logger.info("Sports daily data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)




    def gold_fact_health(self):
        try:
            silver_df = self.spark.read\
                        .option("recursiveFileLookup", True)\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_HEALTH_DIR}/main", mode="ignore")
            logger.info("Health data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)




    def gold_daily_fact_health(self):
        try:
            if os.path.exists(f"{self.config.GOLD_FACT_HEALTH_DIR}/{self.date}"):
                logger.info("Data is already there so skipping")
                return 
    
            silver_df = self.spark.read\
                        .parquet(f"{self.config.SILVER_TRANSFORM_DIR}/{self.date}")
    
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
    
            business_df.write.parquet(f"{self.config.GOLD_FACT_HEALTH_DIR}/{self.date}", mode="ignore")
            logger.info("Health dialy data is written")
    
        except Exception as e:
            self.spark.stop()
            logger.info("Spark session is stopped")
            logger.info(e)
