"""
KPI (Key Performance Indicator) writing utilities for Spark DataFrames.

This module provides functions to persist computed KPI metrics to parquet files
with consistent naming, logging, and performance tracking. Used throughout the
pipeline to save aggregated analytics results.
"""

from pyspark.sql import DataFrame
from src.utils.logger import setup_logger
import time


def write_kpi(
    df: DataFrame,
    kpi_name: str,
    base_path: str = "../data/metrics",
    mode: str = "overwrite"
) -> str:
    """
    Write a Spark DataFrame containing KPI metrics to a Parquet file.

    Persists computed KPI results with automatic directory creation, logging,
    and performance timing. The function is optimized for consistent KPI storage
    across the pipeline.

    Args:
        df (pyspark.sql.DataFrame): DataFrame containing KPI metrics to persist.
                                   Should be a pre-aggregated result set.
        kpi_name (str): Name of the KPI metric. Used for directory and logging.
                       Example: "director_success_metrics"
        base_path (str): Root directory for KPI storage.
                        Defaults to "/data/metrics".
                        Full path will be: {base_path}/{kpi_name}/
        mode (str): Write mode for Spark. Options: "overwrite", "append", "ignore", "error".
                   Defaults to "overwrite" (replace existing KPI files).

    Returns:
        str: Full path to the written KPI directory.
            Example: "/data/metrics/director_success_metrics"

    Note:
        - Execution time is logged for performance monitoring
        - Null values in the DataFrame are preserved in output
        - Output uses Parquet format for efficient compression and querying
        - Directories are created automatically if they don't exist
    """
    # Initialize logger for this operation
    logger = setup_logger("kpi_writer")

    # Record start time to measure write performance
    start = time.time()
    
    # Construct full output path
    output_path = f"{base_path}/{kpi_name}"

    logger.info(f"Persisting KPI '{kpi_name}' to {output_path}")

    # Write DataFrame to Parquet files with specified mode
    # Parquet provides compression and efficient columnar storage
    (
        df.write
        .mode(mode)  # Overwrite existing files or append/ignore based on mode
        .parquet(output_path)
    )

    # Calculate elapsed time for performance logging
    duration = round(time.time() - start, 2)
    logger.info(f"KPI '{kpi_name}' written successfully in {duration}s")

    return output_path


def main():
    """
    Main entry point for KPI writer module.

    Can be used to test KPI writing functionality during development.
    """
    pass