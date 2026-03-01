"""Reusable KPI utilities for ranking, filtering, and metric computation.

This module provides generic helpers that can be applied across any KPI module
to avoid duplication of ranking logic, filtering, and result presentation.
All operations use native Spark APIs and preserve lazy evaluation.
"""

from __future__ import annotations

import time
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from src.utils.logger import setup_logger

# logger for this module
logger = setup_logger(
    name="kpi_utils",
    log_file="/logs/kpi_utils.log",
)


def rank_by_metric(
    df: DataFrame,
    metric: str,
    ascending: bool = False,
    filter_condition: Optional[Column] = None,
    top_n: Optional[int] = None,
) -> DataFrame:
    """Rank and optionally filter a DataFrame by a single numeric metric.

    This is a reusable helper for computing rankings across any KPI module.
    It validates the metric column, applies optional filtering, sorts in the
    specified direction, and optionally limits results to top N rows.

    All transformations are lazy (deferred computation) and use only
    Spark-native functions for maximum Catalyst optimization.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataset containing the metric column to rank by.
    metric : str
        Column name to rank on. Must exist in ``df``.  Should be numeric.
    ascending : bool, default ``False``
        Sort direction. ``False`` = descending (highest first), ``True`` =
        ascending (lowest first).
    filter_condition : pyspark.sql.column.Column | None, optional
        If provided, dataframe is filtered with this condition before sorting.
        The original frame is not mutated; a new lazy frame is returned.
    top_n : int | None, optional
        If provided, apply ``.limit(top_n)`` after sorting to keep only the
        top N rows. Must be > 0 if specified.

    Returns
    -------
    pyspark.sql.DataFrame
        Ranked and optionally filtered/limited DataFrame. All rows sorted by
        ``metric`` in the specified direction.

    Raises
    ------
    ValueError
        When any of the following occurs:

        * ``metric`` column is missing from ``df``
        * ``top_n`` is provided and <= 0
        * ``filter_condition`` is invalid (checked by Spark at execution time)
    """

    start = time.time()
    logger.info(
        "Starting rank_by_metric metric=%s ascending=%s filter=%s top_n=%s",
        metric,
        ascending,
        filter_condition is not None,
        top_n,
    )

    # Validate metric column exists
    if metric not in df.columns:
        raise ValueError(f"Metric column '{metric}' not found in DataFrame")

    # Validate top_n if provided
    if top_n is not None and top_n <= 0:
        raise ValueError(f"top_n must be > 0; got {top_n}")

    # Apply filter if provided
    result = df
    if filter_condition is not None:
        result = result.filter(filter_condition)
        logger.info("Applied filter condition")

    # Apply sorting
    if ascending:
        result = result.orderBy(F.col(metric).asc())
        logger.info("Sorted by %s ascending", metric)
    else:
        result = result.orderBy(F.col(metric).desc())
        logger.info("Sorted by %s descending", metric)

    # Apply top_n limit if provided
    if top_n is not None:
        result = result.limit(top_n)
        logger.info("Limited to top %d rows", top_n)

    duration = time.time() - start
    logger.info("rank_by_metric completed in %.2f seconds", duration)
    return result
