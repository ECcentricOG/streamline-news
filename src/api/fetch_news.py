import json
import os

from dotenv import load_dotenv
from pathlib import Path
from datetime import date
from api.api_client import get_news
from utils.logger import get_logger

load_dotenv()

RAW_DATA_DIR = Path(str(os.getenv("RAW_API_DATA_DIR")))

logger = get_logger(__name__)

def fetch_todays_news():
    download_date = date.today().isoformat()
    logger.info(f"Fetching Todays News download date : {download_date}")

    file_path = RAW_DATA_DIR / f"{download_date}.json"
    if file_path.exists():
        logger.info("Today's file already exists so skipping download")
        return
    logger.info("Fetching news from api")
    
    news_data = get_news()

    output_data = {
        "download_date" : download_date,
        "source" : "API",
        "data" : news_data
    }

    with open(file_path, "w", encoding="utf8") as file:
        json.dump(output_data, file, indent=4, ensure_ascii=False)

    articles = news_data.get("articles", [])
    logger.info(f"Articles fetched : {len(articles)}")
    logger.info(f"File saved       : {file_path}")
    return articles
