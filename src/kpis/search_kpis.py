"""
Advanced movie search and filtering KPIs.

This module implements specialized search queries for discovering movies based on
specific actor, director, and genre combinations. Useful for content discovery,
recommendation systems, and exploratory data analysis.

Functions:
    advanced_movie_search_kpis: Compute filtered result sets for movie searches
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from src.utils.logger import setup_logger


def _validate_columns(df: DataFrame, columns: list[str]) -> None:
    """
    Ensure specified columns exist in the DataFrame.

    Args:
        df (DataFrame): Input Spark DataFrame.
        columns (list[str]): List of column names that must be present.

    Raises:
        ValueError: If any column is missing from the DataFrame.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for filter: {missing}")


def _lower_col(col_name: str) -> Column:
    """
    Return a null-safe lower-case expression for a column.

    Uses :pyfunc:`coalesce` to convert nulls to empty strings before lowering,
    avoiding repeated null checks in filter logic.
    """
    return F.lower(F.coalesce(F.col(col_name), F.lit("")))


def filter_movies(
    df: DataFrame,
    genres: list[str] | None = None,
    actors: list[str] | None = None,
    director: str | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
    **extra_filters
) -> DataFrame:
    """
    Apply a flexible set of filters to the movie DataFrame.

    Supports filtering by multiple genres (AND semantics), multiple actors,
    director name, and arbitrary additional column-based conditions. It also
    handles dynamic sorting and null-safe case-insensitive matching.

    The function validates required columns and builds a single conjunctive
    condition that is applied lazily to the DataFrame. This centralizes
    filtering logic and makes future extensions easy.

    Args:
        df (DataFrame): Input Spark DataFrame containing movie records.
        genres (list[str] | None): List of genres that must all be present.
        actors (list[str] | None): List of actor names to match in cast.
        director (str | None): Director name to match.
        sort_by (str | None): Column name to sort by after filtering.
        ascending (bool): Sort order; ``True`` for ascending, ``False`` for desc.
        **extra_filters: Additional keyword filters mapping column to a value
                         or callable returning a Column expression. Enables
                         extensibility (e.g., rating thresholds, date ranges).

    Returns:
        DataFrame: Filtered (and possibly ordered) DataFrame.
    """
    # collect required columns based on provided arguments
    required_cols: list[str] = []
    if genres:
        required_cols.append("genres")
    if actors:
        required_cols.append("cast")
    if director:
        required_cols.append("director")
    if sort_by:
        required_cols.append(sort_by)
    required_cols.extend(extra_filters.keys())

    if required_cols:
        _validate_columns(df, required_cols)

    # build conditions dynamically
    conditions: list[Column] = []

    if genres:
        # require that each requested genre appears in the genres column
        genre_col = _lower_col("genres")
        cond = None
        for g in genres:
            single = genre_col.contains(g.lower())
            cond = single if cond is None else cond & single
        conditions.append(cond)

    if actors:
        cast_col = _lower_col("cast")
        cond = None
        for a in actors:
            single = cast_col.contains(a.lower())
            cond = single if cond is None else cond & single
        conditions.append(cond)

    if director:
        dir_col = _lower_col("director")
        conditions.append(dir_col.contains(director.lower()))

    # additional user-provided filters
    for col, val in extra_filters.items():
        if callable(val):
            conditions.append(val(F.col(col)))
        else:
            # assume simple equality for atomic values
            conditions.append(F.col(col) == val)

    # combine all conditions into one
    if conditions:
        final_cond = conditions[0]
        for other in conditions[1:]:
            final_cond = final_cond & other
        filtered = df.filter(final_cond)
    else:
        filtered = df

    # apply sorting if requested
    if sort_by:
        order_expr = F.col(sort_by).asc() if ascending else F.col(sort_by).desc()
        filtered = filtered.orderBy(order_expr)

    return filtered

