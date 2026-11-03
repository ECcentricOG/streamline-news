import json

from database.db_connection import get_connection
from utils.app_config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__, "database")

def insert(config: AppConfig,table:str = "news_dataset",):
    FILE_PATH = config.RAW_DATASET_FILE_PATH
    conn = get_connection(config)
    logger.info("Connected to Database")
    cursor = conn.cursor()
    logger.info("Database Cursor is Ready")

    data = []
    with open(str(FILE_PATH), "r", encoding="utf8") as file:
        for line in file:
            if line.strip():
                data.append(json.loads(line))

    insert_query = f"""
    Insert Into {table}(
        link,
        headline,
        category,
        short_description,
        authors,
        date
    ) Values (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    );
    """

    for row in data:
        cursor.execute(
            insert_query,
            (
                row["link"],
                row["headline"],
                row["category"],
                row["short_description"],
                row["authors"],
                row["date"]
            )
        )

    logger.info(f"Data is inserted into the Database of table name : {table}")
    conn.commit()
    logger.info("Changes are commited into Database")
    cursor.close()
    logger.info("Cursor is Closed")
    conn.close()
    logger.info("Database Connection is closed")
