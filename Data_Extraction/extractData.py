class ApiRequestError(Exception):
    """Base class for API request errors."""

import pandas as pd
import requests
import json
import os
from typing import Optional


class ApiRequestError(Exception):
    """Base class for API request errors."""
    pass


def extractDataFromAPIOld(session: Optional[requests.Session], 
                       url: str, 
                       API_KEY: str,
                       movie_ids: list,
                       output_path: str = "/data/raw_data/movieData.csv") -> None:
    
    """

    Queries an API (Movie Dataset API), extracts the dataset and convert it to a pandas dataFrame

    Parameters
    ----------
    sesssion    :   Session Object
                    Session object to handle retry logic

    url :   str
        The URL for the API

    API_KEY : str
            The API KEY for authentication

    movie_ids   :   list
                List of IDs for movies to be extracted

    maxPages    :   int
                Sets the default to 500
                The maximum number of pages to fetch

    Returns:
    -------
    None

    """

    #  VALIDATION 
    if url is None or API_KEY is None:
        raise ValueError("URL and API Key are required.")

    if not movie_ids:
        raise ValueError("Movie ID list cannot be empty")

    url = url.strip()
    API_KEY = API_KEY.strip()

    #  REQUEST SETTINGS 
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    params = {
        "include_adult": True,
        "language": "en-US",
        "append_to_response": "credits"  # includes cast + crew
    }

    df = pd.DataFrame()

    # FETCH MOVIE DATA 
    for movie_id in movie_ids:

        endpoint = f"{url}/{movie_id}"

        try:
            response = session.get(
                url=endpoint,
                headers=headers,
                params=params,
                timeout=10
            )
        except requests.exceptions.Timeout:
            raise ApiRequestError("Request timed out.")
        except requests.exceptions.ConnectionError:
            raise ApiRequestError("Connection error.")
        except requests.exceptions.RequestException as e:
            raise ApiRequestError(f"Network error: {e}")

        if response.status_code == 404:
            # Skip missing movie IDs
            continue

        # Safe JSON parse
        try:
            json_data = response.json()
        except ValueError:
            continue

        movie_df = pd.json_normalize(json_data)
        df = pd.concat([df, movie_df], ignore_index=True)

    # SAVE TO CSV 

    # Create directory if missing
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df.to_csv(output_path, index=False)

    print(f"Movie data saved to: {output_path}")


def extractDataFromAPI(
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
    #print(data.head())

if __name__ == "__main__":
        main()