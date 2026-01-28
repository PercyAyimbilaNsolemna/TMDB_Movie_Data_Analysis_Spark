import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import setup_logger


def director_success_kpis(df: DataFrame) -> dict:
    """
    Computes KPIs for most successful directors.

    Metrics:
        - Total number of movies directed
        - Total revenue
        - Mean rating

    Returns:
        dict[str, DataFrame]: KPI name -> Spark DataFrame
    """

    logger = setup_logger(
        name="director_success_kpis",
        log_file="/logs/director_success_kpis.log"
    )

    start_time = time.time()
    logger.info("Starting Director Success KPI computation")

    # -------------------------------------------------
    # Single aggregation (source of truth)
    # -------------------------------------------------
    logger.info("Aggregating director-level metrics")

    aggregated_df = (
        df
        .filter(F.col("director").isNotNull())
        .groupBy("director")
        .agg(
            F.count("*").alias("total_movies"),
            F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
            F.round(F.avg("vote_average"), 2).alias("mean_rating")
        )
    )

    results = {}

    # -------------------------------------------------
    # KPI 1: Total number of movies directed
    # -------------------------------------------------
    logger.info("Computing total number of movies per director")

    results["total_movies_per_director"] = (
        aggregated_df
        .select("director", "total_movies")
        .orderBy(F.col("total_movies").desc())
    )

    # -------------------------------------------------
    # KPI 2: Total revenue
    # -------------------------------------------------
    logger.info("Computing total revenue per director")

    results["total_revenue_per_director"] = (
        aggregated_df
        .select("director", "total_revenue_musd")
        .orderBy(F.col("total_revenue_musd").desc())
    )

    # -------------------------------------------------
    # KPI 3: Mean rating
    # -------------------------------------------------
    logger.info("Computing mean rating per director")

    results["mean_rating_per_director"] = (
        aggregated_df
        .select("director", "mean_rating")
        .orderBy(F.col("mean_rating").desc())
    )

    # -------------------------------------------------
    # Timing
    # -------------------------------------------------
    duration = time.time() - start_time
    logger.info(
        f"Director Success KPIs completed in {duration:.2f} seconds"
    )

    return results
