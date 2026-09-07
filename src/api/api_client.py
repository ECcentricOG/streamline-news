import os
import requests
from dotenv import load_dotenv

from utils.logger import get_logger

load_dotenv()

API_KEY = os.getenv("API_KEY")

BASE_URL = "https://newsapi.org/v2/top-headlines"

logger = get_logger(__name__)

def get_news():
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
