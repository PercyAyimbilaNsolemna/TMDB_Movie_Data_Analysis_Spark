from pyspark.sql import DataFrame
from src.utils.logger import setup_logger
import time


def write_kpi(
    df: DataFrame,
    kpi_name: str,
    base_path: str = "/data/metrics",
    mode: str = "overwrite"
) -> str:
    logger = setup_logger("kpi_writer")

    start = time.time()
    output_path = f"{base_path}/{kpi_name}"

    logger.info(f"Persisting KPI '{kpi_name}' to {output_path}")

    (
        df.write
        .mode(mode)
        .parquet(output_path)
    )

    duration = round(time.time() - start, 2)
    logger.info(f"KPI '{kpi_name}' written successfully in {duration}s")

    return output_path


def main():
    ...


if __name__ == "__main__":
    main()