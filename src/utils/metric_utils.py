"""Utility helpers for financial metric computations using Spark DataFrames.

This module provides reusable transformations that can be applied lazily on
movie datasets before KPI computation. All operations use native Spark SQL
functions to ensure vectorized, distributed execution without UDF overhead.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, round as spark_round


def compute_financial_metrics(df: DataFrame) -> DataFrame:
    """Add standardized financial columns to a movie DataFrame.

    The transformation is intentionally lazy (returns a new DataFrame with
    additional columns) so it can be composed into larger Spark pipelines.

    The following columns are appended if and only if the required source
    fields exist in ``df``:

    * ``profit`` – calculated as ``revenue_musd - budget_musd``.  The value is
      rounded to two decimal places.  The expression is only evaluated when
      both ``revenue_musd`` and ``budget_musd`` are not ``NULL``; otherwise the
      result is ``NULL``.

    * ``roi`` – return on investment defined as
      ``revenue_musd / budget_musd``.  Computation occurs only when
      ``budget_musd`` is non-``NULL`` and strictly greater than zero to avoid
      divide-by-zero semantics; outcomes are rounded to two decimals.  If the
      condition is not met the column contains ``NULL``.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataframe that should contain numeric columns
        ``revenue_musd`` and ``budget_musd``.  Other columns are left
        untouched.

    Returns
    -------
    pyspark.sql.DataFrame
        A new DataFrame with ``profit`` and ``roi`` columns appended.

    Raises
    ------
    ValueError
        If the input dataframe's schema does not contain both
        ``revenue_musd`` and ``budget_musd``.  The error message lists the
        missing columns to help diagnose upstream issues.
    """

    required: List[str] = ["revenue_musd", "budget_musd"]
    missing: List[str] = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Cannot compute financial metrics; missing columns: {missing}"
        )

    # profit: only when both fields are present and non-null
    profit_expr = when(
        col("revenue_musd").isNotNull() & col("budget_musd").isNotNull(),
        spark_round(col("revenue_musd") - col("budget_musd"), 2),
    )

    # roi: avoid divide-by-zero and require budget > 0
    roi_expr = when(
        col("budget_musd").isNotNull() & (col("budget_musd") > 0),
        spark_round(col("revenue_musd") / col("budget_musd"), 2),
    )

    return df.withColumn("profit", profit_expr).withColumn("roi", roi_expr)
