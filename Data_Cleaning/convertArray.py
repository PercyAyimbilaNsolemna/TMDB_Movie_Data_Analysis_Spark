from pyspark.sql import functions as F
from pyspark.sql.column import Column


def separateArrayColumn(
    array_col: Column,
    key: str,
    separator: str = "|"
) -> Column:
    """
    Converts an array<struct> column into a pipe-separated string
    using a specified struct key.
    """

    return F.when(
        array_col.isNull(),
        F.lit(None)
    ).otherwise(
        F.array_join(
            F.transform(
                array_col,
                lambda x: x.getField(key)
            ),
            separator
        )
    )
