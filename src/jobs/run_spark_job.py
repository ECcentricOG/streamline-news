import argparse
import logging
import re
import sys
from datetime import datetime

from src.utils.app_config import AppConfig


logger = logging.getLogger(__name__)


def validate_processing_date(processing_date: str | None) -> None:
    """Validate the optional processing date."""
    if processing_date is None:
        return

    try:
        datetime.strptime(processing_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid processing date: {processing_date}. "
            "Expected YYYY-MM-DD."
        )


def run_job(stage: str, processing_date: str | None = None) -> None:
    """
    Run the requested pipeline stage.

    PySpark-dependent imports are intentionally inside this function.
    This allows commands such as --help to work without PySpark installed.
    """

    # Import Spark-dependent modules only when an actual job is executed.
    from src.utils.spark_sesssion_factory import SparkSessionFactory
    from src.pipeline.bronze_pipeline import BronzePipeline
    from src.pipeline.silver_pipeline import SilverPipeline
    from src.pipeline.gold_pipeline import GoldPipeline

    config = AppConfig()

    spark = SparkSessionFactory.create(
        app_name=f"streamline-news-{stage}"
    )

    try:
        bronze_pipeline = BronzePipeline(spark)
        silver_pipeline = SilverPipeline(spark)
        gold_pipeline = GoldPipeline(spark)

        stage_functions = {
            "bronze_database_ingestion":
                bronze_pipeline.broze_ingest_database,

            "bronze_api_ingestion":
                bronze_pipeline.bronze_ingest_api,

            "bronze_merge":
                bronze_pipeline.bronze_merge,

            "bronze_daily_merge":
                bronze_pipeline.bronze_daily_merge,

            "silver_ingestion":
                silver_pipeline.silver_ingestion,

            "silver_daily_ingestion":
                silver_pipeline.silver_daily_ingestion,

            "silver_transformation":
                silver_pipeline.silver_transformation,

            "silver_daily_transformation":
                silver_pipeline.silver_daily_transformation,

            "gold_business":
                gold_pipeline.gold_business,

            "gold_daily_business":
                gold_pipeline.gold_daily_business,

            "gold_technology":
                gold_pipeline.gold_technology,

            "gold_daily_technology":
                gold_pipeline.gold_daily_technology,

            "gold_sport":
                gold_pipeline.gold_sport,

            "gold_daily_sport":
                gold_pipeline.gold_daily_sport,

            "gold_health":
                gold_pipeline.gold_health,

            "gold_daily_health":
                gold_pipeline.gold_daily_health,
        }

        if stage not in stage_functions:
            raise ValueError(f"Unsupported pipeline stage: {stage}")

        logger.info(
            "Starting pipeline stage: %s | Processing date: %s",
            stage,
            processing_date,
        )

        # Pass processing_date only if your pipeline methods accept it.
        # Otherwise, call the method without arguments.
        stage_functions[stage]()

        logger.info("Pipeline stage completed: %s", stage)

    finally:
        spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Streamline News Spark pipeline jobs."
    )

    parser.add_argument(
        "--stage",
        required=True,
        choices=[
            "bronze_database_ingestion",
            "bronze_api_ingestion",
            "bronze_merge",
            "bronze_daily_merge",
            "silver_ingestion",
            "silver_daily_ingestion",
            "silver_transformation",
            "silver_daily_transformation",
            "gold_business",
            "gold_daily_business",
            "gold_technology",
            "gold_daily_technology",
            "gold_sport",
            "gold_daily_sport",
            "gold_health",
            "gold_daily_health",
        ],
        help="Pipeline stage to execute.",
    )

    parser.add_argument(
        "--processing-date",
        required=False,
        default=None,
        help="Processing date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    args = parse_args()

    validate_processing_date(args.processing_date)

    run_job(
        stage=args.stage,
        processing_date=args.processing_date,
    )


if __name__ == "__main__":
    main()

