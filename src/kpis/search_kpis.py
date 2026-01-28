import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import setup_logger


def advanced_movie_search_kpis(df: DataFrame) -> dict:
    """
    Advanced filtering & search KPIs.

    Returns:
        dict[str, DataFrame]
    """

    logger = setup_logger(
        name = "advanced_movie_search_kpis",
        log_file = "/logs/advanced_movie_search_kpis.log"
        )
    start_time = time.time()

    logger.info("Starting Advanced Movie Search KPIs")

    results = {}

    # ---------------------------------------------------
    # Search 1:
    # Best-rated Science Fiction Action movies
    # starring Bruce Willis
    # ---------------------------------------------------
    logger.info(
        "Search 1: Sci-Fi Action movies starring Bruce Willis (sorted by rating)"
    )

    search_1 = (
        df
        .filter(
            F.lower(F.col("genres")).contains("science fiction") &
            F.lower(F.col("genres")).contains("action") &
            F.lower(F.col("cast")).contains("bruce willis")
        )
        .orderBy(F.col("vote_average").desc())
    )

    results["bruce_willis_scifi_action"] = search_1

    # ---------------------------------------------------
    # Search 2:
    # Movies starring Uma Thurman
    # directed by Quentin Tarantino
    # sorted by runtime (shortest → longest)
    # ---------------------------------------------------
    logger.info(
        "Search 2: Uma Thurman movies directed by Quentin Tarantino"
    )

    search_2 = (
        df
        .filter(
            F.lower(F.col("cast")).contains("uma thurman") &
            F.lower(F.col("director")).contains("quentin tarantino")
        )
        .orderBy(F.col("runtime").asc())
    )

    results["uma_thurman_tarantino"] = search_2

    # ---------------------------------------------------
    # Timing
    # ---------------------------------------------------
    duration = time.time() - start_time
    logger.info(
        f"Advanced Movie Search KPIs completed in {duration:.2f} seconds"
    )

    return results
