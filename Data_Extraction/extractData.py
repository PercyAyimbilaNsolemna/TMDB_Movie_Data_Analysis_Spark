import sys
import pandas as pd
import requests
import json
import os
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.utils.logger import setup_logger


class ApiRequestError(Exception):
    """Base class for API request errors."""
    pass


# -----------------------------
# Logging setup
# -----------------------------
logger = setup_logger(
    name="data_extraction",
    log_file="/logs/data_extraction.log"
)

logger.info("========== STARTING DATA EXTRACTION ==========")


# -----------------------------
# Fetch movies
# -----------------------------
def fetch_movie(session, endpoint, headers, params, movie_id):
    try:
        response = session.get(
            f"{endpoint}/{movie_id}",
            headers=headers,
            params=params,
            timeout=10
        )
        if response.status_code != 200:
            return None
        return response.json()
    except requests.exceptions.RequestException:
        return None


# -----------------------------
# Extract movie data from the API
# -----------------------------
def extractDataFromAPI(
    session: Optional[requests.Session],
    url: str,
    API_KEY: str,
    movie_ids: list,
    output_path: str = "../data/raw_data/movieData.json",
    max_workers: int = 5
):
    if not url or not API_KEY:
        raise ValueError("URL and API Key required")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    params = {
        "language": "en-US",
        "append_to_response": "credits"
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    results = []

    logger.info("Starting API data extraction and writing to file")

    # -----------------------------
    # Setups up multithreading and writes to file if any of the threads completes the API call
    # -----------------------------
    with open(output_path, "w", encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(fetch_movie, session, url, headers, params, movie_id)
                for movie_id in movie_ids
            ]

            for future in as_completed(futures):
                result = future.result()
                if result:
                    f.write(json.dumps(result) + "\n")

    logger.info("========== DATA EXTRACTION COMPLETED ==========")

    print(f"Saved {len(results)} movies to {output_path}")


def extractDataFromAPIOld(
    session: Optional[requests.Session],
    url: str,
    API_KEY: str,
    movie_ids: list,
    output_path: str = "../data/raw_data/movieData.json"
):
    if not url or not API_KEY:
        raise ValueError("URL and API Key required")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    params = {
        "language": "en-US",
        "append_to_response": "credits"
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    logger.info("Starting API data extraction and writing to file")
    with open(output_path, "w", encoding="utf-8") as f:
        for movie_id in movie_ids:
            endpoint = f"{url}/{movie_id}"

            try:
                response = session.get(
                    endpoint,
                    headers=headers,
                    params=params,
                    timeout=10
                )
                if response.status_code != 200:
                    continue

                json_data = response.json()

                # WRITE RAW JSON (1 record per line)
                f.write(json.dumps(json_data) + "\n")

            except requests.exceptions.RequestException:
                continue
    logger.info("Completed API data extraction and writing to file")

    print(f"Raw TMDB JSON saved to {output_path}")



    

def main():
    import os
    import sys

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    from config.config import loadEnv, getURL, create_retry

    url = getURL()
    API_KEY = loadEnv(fileName="API_KEY")
    movie_ids = [299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861,
                    284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]

    data = extractDataFromAPI(session=create_retry(), url=url, API_KEY=API_KEY, movie_ids=movie_ids)
    print(data.head())

if __name__ == "__main__":
        main()