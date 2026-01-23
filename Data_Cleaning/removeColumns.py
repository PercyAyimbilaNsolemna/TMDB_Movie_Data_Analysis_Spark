from pyspark.sql import DataFrame
import logging


def dropColumns(
    df: DataFrame,
    columns: tuple
) -> DataFrame:
    """
    Drops specified columns from a Spark DataFrame.

    :param df: Input Spark DataFrame
    :param columns: Tuple of column names to drop
    :param logger: Logger instance
    :return: DataFrame with specified columns removed
    """

    try:
        if not isinstance(columns, tuple):
            raise TypeError("columns must be a tuple of strings")

        df_columns = set(df.columns)
        cols_to_drop = set(columns)

        missing_cols = cols_to_drop - df_columns

        if missing_cols:
            logging.warning(
                f"Attempting to drop non-existent columns: {missing_cols}"
            )

        valid_cols = list(cols_to_drop & df_columns)

        logging.info(f"Dropping columns: {valid_cols}")

        return df.drop(*valid_cols)

    except Exception as e:
        logging.exception("Failed while dropping columns from DataFrame")
        raise


def main():
    ...



if __name__ == "__main__":
    main()