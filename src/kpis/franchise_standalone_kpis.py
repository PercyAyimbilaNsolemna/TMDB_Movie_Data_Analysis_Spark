import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import setup_logger


def franchise_vs_standalone_kpis(df: DataFrame) -> DataFrame:
    """
    Compare Franchise vs Standalone movie performance.

    Returns:
        Spark DataFrame with aggregated metrics
    """

    logger = setup_logger(
        name = "franchise_vs_standalone_kpis",
        log_file = "/logs/franchise_vs_standalone_kpis.log")
    start_time = time.time()

    logger.info("Starting Franchise vs Standalone KPI computation")

    # ---------------------------------------------------
    # Label movies
    # ---------------------------------------------------
    df_labeled = df.withColumn(
        "movie_type",
        F.when(F.col("belongs_to_collection").isNotNull(), "Franchise")
         .otherwise("Standalone")
    )

    logger.info("Movies labeled as Franchise or Standalone")

    # ---------------------------------------------------
    # Aggregations
    # ---------------------------------------------------
    summary_df = (
        df_labeled
        .groupBy("movie_type")
        .agg(
            F.round(F.mean("revenue_musd"), 2).alias("mean_revenue"),
            F.round(F.expr("percentile_approx(roi, 0.5)"), 2).alias("median_roi"),
            F.round(F.mean("budget_musd"), 2).alias("mean_budget"),
            F.round(F.mean("popularity"), 2).alias("mean_popularity"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating")
        )
    )

    # ---------------------------------------------------
    # Timing
    # ---------------------------------------------------
    duration = time.time() - start_time
    logger.info(
        f"Franchise vs Standalone KPIs completed in {duration:.2f} seconds"
    )

    return summary_df
