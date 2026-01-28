import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import setup_logger


def franchise_success_kpis(df: DataFrame) -> dict:
    """
    Computes KPIs for most successful movie franchises.

    Metrics:
        - Total number of movies in franchise
        - Total budget
        - Mean budget
        - Total revenue
        - Mean revenue
        - Mean rating

    Returns:
        dict[str, DataFrame]: KPI name -> Spark DataFrame
    """

    logger = setup_logger(
        name="franchise_success_kpis",
        log_file="/logs/franchise_success_kpis.log"
    )

    start_time = time.time()
    logger.info("Starting Franchise Success KPI computation")

    # -------------------------------------------------
    # Single aggregation (source of truth)
    # -------------------------------------------------
    logger.info("Aggregating franchise-level metrics")

    aggregated_df = (
        df
        .filter(F.col("belongs_to_collection").isNotNull())
        .groupBy("belongs_to_collection")
        .agg(
            F.count("*").alias("total_movies"),
            F.round(F.sum("budget_musd"), 2).alias("total_budget_musd"),
            F.round(F.avg("budget_musd"), 2).alias("mean_budget_musd"),
            F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
            F.round(F.avg("revenue_musd"), 2).alias("mean_revenue_musd"),
            F.round(F.avg("vote_average"), 2).alias("mean_rating")
        )
    )

    results = {}

    # -------------------------------------------------
    # KPI 1: Total number of movies in franchise
    # -------------------------------------------------
    logger.info("Computing total number of movies per franchise")

    results["total_movies_per_franchise"] = (
        aggregated_df
        .select("belongs_to_collection", "total_movies")
        .orderBy(F.col("total_movies").desc())
    )

    # -------------------------------------------------
    # KPI 2: Total budget
    # -------------------------------------------------
    logger.info("Computing total budget per franchise")

    results["total_budget_per_franchise"] = (
        aggregated_df
        .select("belongs_to_collection", "total_budget_musd")
        .orderBy(F.col("total_budget_musd").desc())
    )

    # -------------------------------------------------
    # KPI 3: Mean budget
    # -------------------------------------------------
    logger.info("Computing mean budget per franchise")

    results["mean_budget_per_franchise"] = (
        aggregated_df
        .select("belongs_to_collection", "mean_budget_musd")
        .orderBy(F.col("mean_budget_musd").desc())
    )

    # -------------------------------------------------
    # KPI 4: Total revenue
    # -------------------------------------------------
    logger.info("Computing total revenue per franchise")

    results["total_revenue_per_franchise"] = (
        aggregated_df
        .select("belongs_to_collection", "total_revenue_musd")
        .orderBy(F.col("total_revenue_musd").desc())
    )

    # -------------------------------------------------
    # KPI 5: Mean revenue
    # -------------------------------------------------
    logger.info("Computing mean revenue per franchise")

    results["mean_revenue_per_franchise"] = (
        aggregated_df
        .select("belongs_to_collection", "mean_revenue_musd")
        .orderBy(F.col("mean_revenue_musd").desc())
    )

    # -------------------------------------------------
    # KPI 6: Mean rating
    # -------------------------------------------------
    logger.info("Computing mean rating per franchise")

    results["mean_rating_per_franchise"] = (
        aggregated_df
        .select("belongs_to_collection", "mean_rating")
        .orderBy(F.col("mean_rating").desc())
    )

    # -------------------------------------------------
    # Timing
    # -------------------------------------------------
    duration = time.time() - start_time
    logger.info(
        f"Franchise Success KPIs completed in {duration:.2f} seconds"
    )

    return results
