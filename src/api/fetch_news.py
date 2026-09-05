import json
from pathlib import Path
from datetime import timedelta, date
from api.api_client import get_news
from utils.logger import get_logger

RAW_DATA_DIR = Path("data/api/raw")

logger = get_logger(__name__)

def fetch_todays_news():
    download_date = date.today()

    news_date = download_date - timedelta(days=1)

    downlaod = download_date.isoformat()
    news = news_date.isoformat()
    logger.info(f"Fetching Todays News download date : {downlaod} with news date : {news}")

    file_path = RAW_DATA_DIR / f"{downlaod}.json"
    if file_path.exists():
        logger.info("Today's file already exists so skipping download")
        return
    logger.info("Fetching news from api")
    
    news_data = get_news(news, news)

    output_data = {
        "download_date" : downlaod,
        "news_date" : news,
        "source" : "API",
        "data" : news_data
    }

    with open(file_path, "w", encoding="utf8") as file:
        json.dump(output_data, file, indent=4, ensure_ascii=False)

    articles = news_data.get("articles", [])
    logger.info(f"Articles fetched : {len(articles)}")
    logger.info(f"File saved       : {file_path}")
    return articles
