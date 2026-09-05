import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

BASE_URL = "https://newsapi.org/v2"

def get_news(from_date, to_date):
    if not API_KEY:
        raise ValueError("API_KEY not found in .env file")
    url = f"{BASE_URL}/everything"

    params = {
            "apikey" : API_KEY,
            "from" : from_date,
            "to" : to_date,
            "language" : "eng",
            "sortBy" : "publishedAt",
            "pageSize" : 100,
            "page" : 1
        }

    resp = requests.get(url=url, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    if data.get("status") != "ok":
        raise RuntimeError(f"NewsAPI error: {data.get('message', 'Unknown error')}")

    return data
