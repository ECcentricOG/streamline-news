import requests
import json

from utils.app_config import AppConfig
from utils.logger import get_logger
from pathlib import Path

logger = get_logger(__name__)

class APIClient:
    def __init__(self, config:AppConfig, date:str) -> None:
        self.config = config
        self.date = date

    def get_news(self):
        API_KEY = str(self.config.API_KEY)
        BASE_URL = str(self.config.BASE_URL)
    
        if not API_KEY:
            raise ValueError("API_KEY not found in .env file")
    
        params = {
                "apikey" : API_KEY,
                "language" : "en",
                "pageSize" : 100,
                "page" : 1
            }
    
        resp = requests.get(url=BASE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            logger.info("News API Response :")
            logger.info(resp.text)
            resp.raise_for_status()
    
        data = resp.json()
    
        if data.get("status") != "ok":
            raise RuntimeError(f"NewsAPI error: {data.get('message', 'Unknown error')}")
    
        logger.info("News API's data has beeen sucessfully fetched")
        return data



    def fetch_todays_news(self):
        RAW_DATA_DIR = Path(str(self.config.RAW_API_DATA_DIR))
        logger.info(f"Fetching Todays News download date : {self.date}")
    
        file_path = RAW_DATA_DIR / f"{self.date}.json"
        if file_path.exists():
            logger.info("Today's file already exists so skipping download")
            return
        logger.info("Fetching news from api")
        
        news_data = self.get_news()
    
        output_data = {
            "download_date" : self.date,
            "source" : "API",
            "data" : news_data
        }
    
        with open(file_path, "w", encoding="utf8") as file:
            json.dump(output_data, file, indent=4, ensure_ascii=False)
    
        articles = news_data.get("articles", [])
        logger.info(f"Articles fetched : {len(articles)}")
        logger.info(f"File saved       : {file_path}")
        return articles
