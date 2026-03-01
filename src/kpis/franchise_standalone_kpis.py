"""Reusable KPI aggregation helpers and metric definitions.

This module centralizes common grouping logic and metric expressions used by the
various KPI modules.  By keeping the dimension aggregation and metric builders
in one place, KPI files remain thin and follow a consistent pattern.  All
operations use native Spark APIs and return lazy DataFrames suitable for
further composition.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from src.utils.logger import setup_logger

# logger for this module
logger = setup_logger(
    name="aggregate_by_dimension",
    log_file="/logs/aggregate_by_dimension.log",
)

# supported aggregation mapping
_AGG_MAP: Dict[str, Any] = {
    "mean": lambda c: F.mean(c),
    "sum": lambda c: F.sum(c),
    "count": lambda c: F.count(c),
    "median": lambda c: F.expr(f"percentile_approx({c}, 0.5)"),
    "max": lambda c: F.max(c),
    "min": lambda c: F.min(c),
}


# --------- aggregation helper ---------------------------------------------

def aggregate_by_dimension(
    df: DataFrame,
    dimension: str,
    metrics: List[Dict[str, Any]],
    min_count: Optional[int] = None,
    order_by: Optional[str] = None,
    ascending: bool = False,
) -> DataFrame:
    """Flexible engine that groups and aggregates according to a declarative
    metric specification.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataset containing all referenced columns (including financial
        metrics like ``profit`` and ``roi`` which must be added upstream).
    dimension : str
        Column to group on; must exist and non-null values are enforced.
    metrics : list of dict
        Each dictionary must contain keys ``column`` and ``agg``; optional
        ``alias`` and ``round`` control the output name and numeric
        precision.  Supported aggregations are defined in :data:`_AGG_MAP`.
    min_count : int | None, optional
        When set, groups with fewer than this many rows are filtered out after
        aggregation.
    order_by : str | None, optional
        Column name in the aggregated result to sort on.
    ascending : bool, default ``False``
        Sort direction when ``order_by`` is specified.

    Returns
    -------
    pyspark.sql.DataFrame
        Dataset summarised by ``dimension`` with the requested metrics.

    Raises
    ------
    ValueError
        When any of the following occurs:

        * ``dimension`` column is missing from ``df``
        * ``metrics`` list is empty or malformed
        * a referenced column in ``metrics`` does not exist
        * an unsupported ``agg`` value is requested
        * ``min_count`` is provided and less than 1
        * ``order_by`` is not present in the result schema
        * duplicate aliases are generated
    """

    start = time.time()
    logger.info(
        "Starting aggregation dimension=%s metrics=%s min_count=%s order_by=%s ascending=%s",
        dimension,
        metrics,
        min_count,
        order_by,
        ascending,
    )

    if dimension not in df.columns:
        raise ValueError(f"Dimension column '{dimension}' not found in DataFrame")

    if not metrics:
        raise ValueError("The metrics list must contain at least one entry")

    if min_count is not None and min_count < 1:
        raise ValueError("min_count must be >= 1 if provided")

    # validate metrics and build expressions
    seen_aliases: set[str] = set()
    exprs: List[Column] = []
    for metric in metrics:
        if "column" not in metric or "agg" not in metric:
            raise ValueError(f"Each metric dict must contain 'column' and 'agg': {metric}")

        col_name = metric["column"]
        agg_name = metric["agg"]
        alias = metric.get("alias")
        precision = metric.get("round", 2)

        if col_name not in df.columns:
            raise ValueError(f"Metric refers to missing column '{col_name}'")

        if agg_name not in _AGG_MAP:
            raise ValueError(f"Unsupported aggregation type '{agg_name}'")

        # build base expression
        base_expr = _AGG_MAP[agg_name](col_name)

        # apply rounding for numeric aggs (count returns long so skip round)
        if agg_name != "count" and precision is not None:
            base_expr = F.round(base_expr, precision)

        alias_name = alias or f"{agg_name}_{col_name}"
        if alias_name in seen_aliases:
            raise ValueError(f"Duplicate alias generated: '{alias_name}'")
        seen_aliases.add(alias_name)
        exprs.append(base_expr.alias(alias_name))

    # drop null dimension keys and aggregate once
    base = df.filter(F.col(dimension).isNotNull())

    # if min_count is specified, add helper count to filter groups
    agg_exprs = list(exprs)
    if min_count is not None:
        agg_exprs.append(F.count("*").alias("_group_count"))

    grouped = base.groupBy(dimension).agg(*agg_exprs)

    if min_count is not None:
        grouped = grouped.filter(F.col("_group_count") >= min_count).drop("_group_count")
        logger.info("Filtered groups: min_count=%d applied", min_count)

    if order_by is not None:
        if order_by not in grouped.columns:
            raise ValueError(f"order_by column '{order_by}' not found in result")
        grouped = grouped.orderBy(F.col(order_by), ascending=ascending)
        logger.info("Sorted by %s ascending=%s", order_by, ascending)

    duration = time.time() - start
    logger.info("Aggregation completed in %.2f seconds", duration)
    return grouped
