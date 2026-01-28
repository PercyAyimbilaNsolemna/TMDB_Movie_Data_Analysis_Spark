import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import setup_logger


def movie_performance_kpis(df: DataFrame) -> dict:
    """
    Computes best/worst performing movie KPIs.

    Returns:
        dict[str, DataFrame]: KPI name -> Spark DataFrame
    """

    # -----------------------------
    # Logging setup
    # -----------------------------
    logger = setup_logger(
        name = "movie_performance_kpis",
        log_file = "/logs/movie_performance_kpis.log"
        )
    
    start_time = time.time()

    logger.info("Starting Movie Performance KPIs computation")

    # -----------------------------
    # Derived metrics (computed once)
    # -----------------------------
    logger.info("Adding derived columns: profit and ROI")

    df_metrics = (
        df
        .withColumn("profit", F.round(F.col("revenue_musd") - F.col("budget_musd"), 2))
        .withColumn(
            "roi",
            F.when(F.col("budget_musd") > 0, F.round(F.col("revenue_musd") / F.col("budget_musd"), 2))
        )
    )

    results = {}

    # -----------------------------
    # Revenue / Budget KPIs
    # -----------------------------
    logger.info("Computing revenue and budget rankings")

    results["highest_revenue"] = df_metrics.orderBy(F.col("revenue_musd").desc())
    results["highest_budget"] = df_metrics.orderBy(F.col("budget_musd").desc())

    # -----------------------------
    # Profit KPIs
    # -----------------------------
    logger.info("Computing profit-based KPIs")

    results["highest_profit"] = df_metrics.orderBy(F.col("profit").desc())
    results["lowest_profit"] = df_metrics.orderBy(F.col("profit").asc())

    # -----------------------------
    # ROI KPIs (Budget >= 10M)
    # -----------------------------
    logger.info("Computing ROI-based KPIs (budget >= 10M)")

    roi_df = df_metrics.filter(F.col("budget_musd") >= 10)

    results["highest_roi"] = roi_df.orderBy(F.col("roi").desc())
    results["lowest_roi"] = roi_df.orderBy(F.col("roi").asc())

    # -----------------------------
    # Votes / Ratings KPIs
    # -----------------------------
    logger.info("Computing votes and ratings KPIs")

    results["most_voted"] = df_metrics.orderBy(F.col("vote_count").desc())

    rated_df = df_metrics.filter(F.col("vote_count") >= 10)

    results["highest_rated"] = rated_df.orderBy(F.col("vote_average").desc())
    results["lowest_rated"] = rated_df.orderBy(F.col("vote_average").asc())

    # -----------------------------
    # Popularity KPI
    # -----------------------------
    logger.info("Computing popularity KPI")

    results["most_popular"] = df_metrics.orderBy(F.col("popularity").desc())

    # -----------------------------
    # Timing
    # -----------------------------
    duration = time.time() - start_time
    logger.info(
        f"Movie Performance KPIs completed in {duration:.2f} seconds"
    )

    return results
