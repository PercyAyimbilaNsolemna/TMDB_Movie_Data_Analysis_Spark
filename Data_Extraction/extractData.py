"""
Data extraction module for TMDB movie API.

This module handles fetching movie data from The Movie Database (TMDB) API
using concurrent HTTP requests. Data is extracted with full credits information
(cast and crew) and saved as newline-delimited JSON (NDJSON) format.

The module supports parallel data extraction approaches:
- extractDataFromAPI: Efficient multi-threaded approach using ThreadPoolExecutor

Key Features:
    - Concurrent HTTP requests using ThreadPoolExecutor
    - Automatic retry logic with configurable timeout
    - Streaming JSON output to minimize memory usage
    - Comprehensive logging of extraction progress

Exceptions:
    ApiRequestError: Custom exception for API request failures
"""

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
    """
    Custom exception class for API request errors.

    Raised when an API request fails due to network issues, invalid responses,
    or other HTTP-related problems that cannot be recovered automatically.
    """
    pass


# Initialize logger for data extraction operations
logger = setup_logger(
    name="data_extraction",
    log_file="/logs/data_extraction.log"
)

logger.info("========== STARTING DATA EXTRACTION ==========")


def fetch_movie(session, endpoint, headers, params, movie_id):
    """
    Fetch a single movie's data from the TMDB API.

    Performs an HTTP GET request to retrieve movie information including
    cast and crew details. Returns None silently on failure (logged elsewhere)
    to allow the ThreadPoolExecutor to continue processing other movies.

    Args:
        session (requests.Session): Requests session with retry logic configured.
        endpoint (str): Base TMDB API endpoint URL for movies.
        headers (dict): HTTP headers including authorization token.
        params (dict): Query parameters (language, append_to_response, etc.).
        movie_id (int): TMDB identifier for the movie to fetch.

    Returns:
        dict or None: Parsed JSON response containing movie data if successful,
                     None if the request fails or returns non-200 status code.

    Note:
        Exceptions are silently caught to allow multi-threaded execution to continue.
        Check logs for details on failed requests.
    """
    try:
        # Construct full endpoint URL with movie ID
        response = session.get(
            f"{endpoint}/{movie_id}",
            headers=headers,
            params=params,
            timeout=10  # 10-second timeout per request
        )
        
        # Return parsed JSON only if status indicates success
        if response.status_code != 200:
            return None
        return response.json()
    except requests.exceptions.RequestException:
        # Silently handle request failures; continue processing other movies
        return None


def extractDataFromAPI(
    session: Optional[requests.Session],
    url: str,
    API_KEY: str,
    movie_ids: list,
    output_path: str = "../data/raw_data/movieData.json",
    max_workers: int = 5
):
    """
    Extract movie data from TMDB API using concurrent requests.

    Fetches data for multiple movies in parallel using ThreadPoolExecutor.
    Results are written to a newline-delimited JSON file immediately as each
    thread completes, minimizing memory usage for large datasets.

    Args:
        session (requests.Session): Requests session with configured retry logic
                                   (typically from create_retry()).
        url (str): Base TMDB API endpoint URL (e.g., "https://api.themoviedb.org/3/movie").
        API_KEY (str): TMDB API authentication key.
        movie_ids (list): List of TMDB movie IDs to fetch.
        output_path (str): File path where NDJSON output will be written.
                          Defaults to "../data/raw_data/movieData.json".
        max_workers (int): Maximum number of concurrent threads.
                          Defaults to 5 (balance between throughput and API rate limits).

    Raises:
        ValueError: If url or API_KEY is empty or None.

    Returns:
        None: Results are written directly to output_path.

    Note:
        - Output format is newline-delimited JSON (NDJSON), one movie per line
        - Failed requests are silently skipped; only successful responses are written
        - HTTP 429 (rate limit) responses trigger automatic retry via session config
        - Output directory is created if it does not exist
    """
    # Validate required parameters
    if not url or not API_KEY:
        raise ValueError("URL and API Key required")

    # Set up HTTP headers for TMDB API authentication
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    # Request credits data alongside movie information
    params = {
        "language": "en-US",
        "append_to_response": "credits"  # Include cast and crew information
    }

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    results = []

    logger.info("Starting API data extraction and writing to file")

    # Use ThreadPoolExecutor for parallel API requests
    # Write results to file immediately as each thread completes to save memory
    with open(output_path, "w", encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all movie fetch tasks to the executor
            futures = [
                executor.submit(fetch_movie, session, url, headers, params, movie_id)
                for movie_id in movie_ids
            ]

            # Process completed futures as they finish (not in submission order)
            # This allows writing results incrementally rather than waiting for all threads
            for future in as_completed(futures):
                result = future.result()
                if result:
                    # Write each successful result as a separate JSON line (NDJSON format)
                    f.write(json.dumps(result) + "\n")

    logger.info("========== DATA EXTRACTION COMPLETED ==========")

    print(f"Saved {len(results)} movies to {output_path}")



    

def main():
    """
    Main entry point for data extraction script.

    Orchestrates the full data extraction workflow:
    1. Load configuration (API endpoint and credentials)
    2. Set up session with retry logic
    3. Extract data for a predefined set of movies
    4. Save results to NDJSON file

    To use: python extractData.py
    """
    import os
    import sys

    # Ensure project root is in path for imports
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    from config.config import loadEnv, getURL, create_retry

    # Load TMDB API configuration
    url = getURL()
    API_KEY = loadEnv(fileName="API_KEY")
    
    # Sample movie IDs for extraction
    # These are popular/well-documented movies with complete cast/crew data
    movie_ids = [299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861,
                    284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]

    # Create session with automatic retry logic for transient failures
    session = create_retry()
    
    # Extract data using parallel requests
    data = extractDataFromAPI(session=session, url=url, API_KEY=API_KEY, movie_ids=movie_ids)
    
    # Note: extractDataFromAPI writes directly to file and doesn't return DataFrame
    # The print statement below is informational
    print("Data extraction complete. Check logs for details.")

if __name__ == "__main__":
        main()