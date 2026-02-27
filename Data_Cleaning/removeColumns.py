"""
Column removal utilities for Spark DataFrames.

This module provides functions to safely drop columns from Spark DataFrames
with validation, error handling, and logging to prevent data loss from
accidentally removing non-existent columns.
"""

from pyspark.sql import DataFrame
import logging


def dropColumns(
    df: DataFrame,
    columns: tuple
) -> DataFrame:
    """
    Drop specified columns from a Spark DataFrame with validation.

    Safely removes columns from a DataFrame while validating column existence.
    Logs warnings if any specified columns don't exist in the DataFrame,
    and only removes the columns that are actually present.

    This defensive approach prevents pipeline failures due to inconsistent
    column presence while providing visibility into data structure issues.

    Args:
        df (pyspark.sql.DataFrame): Input Spark DataFrame.
        columns (tuple): Tuple of column names to drop. Must be a tuple of strings.

    Returns:
        pyspark.sql.DataFrame: New DataFrame with specified columns removed.
                              Original DataFrame is unchanged (Spark is immutable).

    Raises:
        TypeError: If columns parameter is not a tuple.
        Exception: Re-raises any unexpected exceptions during column dropping.

    Note:
        - Non-existent columns are skipped and logged as warnings
        - If all specified columns exist and are valid, info-level logging is used
        - The function is safe to call multiple times with overlapping column sets
    """

    try:
        # Validate that columns parameter is a tuple as expected
        if not isinstance(columns, tuple):
            raise TypeError("columns must be a tuple of strings")

        # Get the set of columns currently in the DataFrame
        df_columns = set(df.columns)
        
        # Convert requested columns to a set for set operations
        cols_to_drop = set(columns)

        # Identify columns that don't exist in the DataFrame
        # This allows us to warn about potential issues in the data pipeline
        missing_cols = cols_to_drop - df_columns

        # Log warning if any requested columns don't exist
        if missing_cols:
            logging.warning(
                f"Attempting to drop non-existent columns: {missing_cols}"
            )

        # Calculate the intersection of requested and actual columns
        # Only drop columns that actually exist
        valid_cols = list(cols_to_drop & df_columns)

        # Log the columns that will actually be dropped
        logging.info(f"Dropping columns: {valid_cols}")

        # Return new DataFrame with columns removed
        return df.drop(*valid_cols)

    except Exception as e:
        # Log the full exception with stack trace for debugging
        logging.exception("Failed while dropping columns from DataFrame")
        raise


def main():
    """
    Main entry point for column removal module testing.

    Can be used to test column removal operations during development.
    """
    pass



if __name__ == "__main__":
    main()