"""
Array transformation utilities for Spark DataFrames.

This module provides functions to convert complex array<struct> columns
into pipe-separated string representations, facilitating downstream data
analysis and visualization by flattening nested structures.
"""

from pyspark.sql import functions as F
from pyspark.sql.column import Column


def separateArrayColumn(
    array_col: Column,
    key: str,
    separator: str = "|"
) -> Column:
    """
    Convert array<struct> column to pipe-separated string values.

    Transforms an array of structs by extracting a specified field from each
    struct and concatenating the values with a delimiter. Handles null arrays
    gracefully by returning null values.

    This is useful for flattening nested TMDB data structures like genres,
    cast, and production companies into human-readable string representations.

    Args:
        array_col (pyspark.sql.column.Column): Array column containing struct elements.
                                               Expected format: array<struct<...>>
        key (str): The field name to extract from each struct in the array.
                   Example: "name" to extract the name field from array<struct>.
        separator (str): String delimiter to join extracted values.
                        Defaults to "|" (pipe character).

    Returns:
        pyspark.sql.column.Column: An expression that produces a string column
                                  with extracted values joined by separator.
                                  Null input arrays produce null output values.

    Example:
        >>> # Transform genres array to pipe-separated genre names
        >>> df_transformed = df.withColumn(
        ...     "genres",
        ...     separateArrayColumn(F.col("genres"), "name")
        ... )
        >>> # Result: "Action|Adventure|Science Fiction"
    """

    return F.when(
        # Return null if the array column is null (preserves null values)
        array_col.isNull(),
        F.lit(None)
    ).otherwise(
        # Extract the specified field from each struct and join with separator
        F.array_join(
            # Apply transformation to extract the key from each struct
            F.transform(
                array_col,
                lambda x: x.getField(key)  # Extract the field named 'key'
            ),
            separator  # Use specified separator between values
        )
    )
