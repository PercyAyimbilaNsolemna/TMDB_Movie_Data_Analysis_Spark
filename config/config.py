"""
Configuration module for TMDB Movie Data Analysis project.

This module handles environment variable loading and API configuration,
providing utilities to access credentials and API endpoints securely
from .env files.

Functions:
    loadEnv: Retrieve environment variables from .env file
    getURL: Get the base TMDB API endpoint URL
    create_retry: Create a retry session with exponential backoff logic
"""


def loadEnv(fileName: str) -> str:
    """
    Load environment variable from .env file.

    Retrieves configuration values (such as API keys) from the .env file
    using python-dotenv. Provides a fallback message if the variable is not found.

    Args:
        fileName (str): The name of the environment variable to retrieve.

    Returns:
        str: The value of the environment variable, or a message indicating
             the variable was not found.
    """
    import os
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    # Retrieve the requested environment variable
    data = os.getenv(fileName)

    # Return data if found, otherwise return informative error message
    return data if data != None else f"{fileName} was not found in the .env file"


def getURL() -> str:
    """
    Get the base TMDB API endpoint URL.

    Returns the root URL for the TMDB (The Movie Database) API v3.
    Specific movie endpoints should be appended to this base URL.

    Returns:
        str: The base TMDB API endpoint URL for movie resources.

    Example:
        base_url = getURL()  # Returns "https://api.themoviedb.org/3/movie"
        full_endpoint = f"{base_url}/299534"  # Specific movie endpoint
    """
    return "https://api.themoviedb.org/3/movie"



def create_retry(total: int = 3, 
                 backoff_factor: float = 0.3, 
                 status_forcelist: tuple = (429, 500, 502, 503, 504), 
                 alllowed_methods: tuple = ("GET", "POST")) -> object:
    """
    Create a requests Session with automatic retry logic and exponential backoff.

    Implements resilient HTTP request handling by automatically retrying failed
    requests with exponential backoff delays. Useful for handling rate limiting
    (429), server errors (5xx), and transient failures.

    Args:
        total (int): Maximum number of retry attempts. Defaults to 3.
        backoff_factor (float): Multiplier for exponential backoff delay calculation.
                                Formula: {backoff_factor} * (2 ** (number_of_retries - 1)).
                                Defaults to 0.3 seconds.
        status_forcelist (tuple): HTTP status codes that trigger a retry.
                                  Defaults to (429, 500, 502, 503, 504).
        alllowed_methods (tuple): HTTP methods allowed to perform retries.
                                  Defaults to ("GET", "POST").

    Returns:
        requests.Session: A configured session object with retry logic mounted
                         to http:// and https:// adapters.

    Example:
        session = create_retry(total=5, backoff_factor=0.5)
        response = session.get("https://api.themoviedb.org/3/movie/299534")
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # Configure retry strategy with specified parameters
    retries = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=alllowed_methods
    )

    # Create an HTTP adapter with the retry strategy
    adapter = HTTPAdapter(max_retries=retries)

    # Create and configure a session
    session = requests.Session()

    # Mount the retry adapter to both HTTP and HTTPS connections
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def main():
    """
    Main entry point for configuration module testing.

    Can be used to verify that environment variables are properly loaded.
    Currently disabled to prevent accidental exposure of API keys.
    """
    # Uncomment below to test environment variable loading during development:
    # print(f"The API key is : \n{loadEnv(fileName='API_KEY')}")
    pass


if __name__ == "__main__":
    main()