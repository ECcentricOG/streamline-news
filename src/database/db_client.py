import psycopg2
import json

from utils.app_config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__, "database")

class DatabaseClient:
    def __init__(self, config:AppConfig, table_name:str="news_dataset") -> None:
        self.config = config
        self.table_name = table_name

    def get_connection(self):
        logger.info("Connecting to Database...")
        return psycopg2.connect(
            host = self.config.DB_HOST,
            user = self.config.DB_USER,
            password = self.config.DB_PASSWORD,
            dbname = self.config.DB_NAME,
            port = self.config.DB_PORT
        )



    def insert(self):
        FILE_PATH = self.config.RAW_DATASET_FILE_PATH
        conn = self.get_connection()
        logger.info("Connected to Database")
        cursor = conn.cursor()
        logger.info("Database Cursor is Ready")
    
        data = []
        with open(str(FILE_PATH), "r", encoding="utf8") as file:
            for line in file:
                if line.strip():
                    data.append(json.loads(line))
    
        insert_query = f"""
        Insert Into {self.table_name}(
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
    
        logger.info(f"Data is inserted into the Database of table name : {self.table_name}")
        conn.commit()
        logger.info("Changes are commited into Database")
        cursor.close()
        logger.info("Cursor is Closed")
        conn.close()
        logger.info("Database Connection is closed")
