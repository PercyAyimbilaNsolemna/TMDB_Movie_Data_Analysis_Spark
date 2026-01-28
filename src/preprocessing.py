#!/usr/bin/env python3
# data_cleaning_spark.py

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# Logger setup (robust for notebooks + Docker)
# --------------------------------------------------
def setup_logger(log_file: str):
    logger = logging.getLogger("preprocessing")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Console handler (for notebook visibility)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


# --------------------------------------------------
# Main preprocessing function
# --------------------------------------------------
def preprocessing(filepath="/data/raw_data/movieData.json"):
    """
    Cleans and preprocesses TMDB movie data using Spark.

    Parameters
    ----------
    filepath : str
        Absolute path to raw movie JSON file inside container.

    Returns
    -------
    str
        Path to cleaned parquet dataset.
    """

    # -----------------------------
    # Logging setup
    # -----------------------------
    log_dir = "/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "data_cleaning.log")

    logger = setup_logger(log_file)
    logger.info("========== STARTING DATA PREPROCESSING ==========")

    # -----------------------------
    # Initialize Spark
    # -----------------------------
    try:
        spark = (
            SparkSession.builder
            .appName("TMDB Movie Data Analysis - Preprocessing")
            .getOrCreate()
        )
        logger.info("Spark session started successfully.")
    except Exception as e:
        logger.error(f"Failed to start Spark session: {e}", exc_info=True)
        raise

    # -----------------------------
    # Load raw data
    # -----------------------------
    try:
        movie_data = (
            spark.read
            .option("inferSchema", "true")
            .json(filepath)
        )
        logger.info(f"Data loaded from {filepath}")
        logger.info(f"Initial row count: {movie_data.count()}")
    except Exception as e:
        logger.error(f"Failed to load data: {e}", exc_info=True)
        spark.stop()
        raise

    # -----------------------------
    # Step 1: Drop irrelevant columns
    # -----------------------------
    try:
        cols_to_drop = [
            "adult",
            "backdrop_path",
            "origin_country",
            "original_title",
            "imdb_id",
            "video",
            "homepage",
        ]
        movie_data = movie_data.drop(
            *[c for c in cols_to_drop if c in movie_data.columns]
        )
        logger.info(f"Dropped irrelevant columns: {cols_to_drop}")
    except Exception as e:
        logger.warning(f"Column drop issue: {e}", exc_info=True)

    # -----------------------------
    # Step 2: Parse array columns
    # -----------------------------
    try:
        array_columns = {
            "genres": "name",
            "production_countries": "name",
            "production_companies": "name",
            "spoken_languages": "english_name",
        }

        for col, key in array_columns.items():
            if col in movie_data.columns:
                movie_data = movie_data.withColumn(
                    col,
                    F.concat_ws("|", F.expr(f"transform({col}, x -> x.{key})"))
                )

        logger.info("Parsed array-based columns successfully.")
    except Exception as e:
        logger.error("Error parsing array columns", exc_info=True)

    # -----------------------------
    # Step 3: Extract collection, director, cast & sizes
    # -----------------------------
    try:
        movie_data = movie_data.withColumn(
            "belongs_to_collection",
            F.col("belongs_to_collection.name")
        )

        movie_data = movie_data.withColumn(
            "director",
            F.expr(
                "concat_ws(', ', transform(filter(credits.crew, x -> x.job = 'Director'), x -> x.name))"
            )
        )

        movie_data = movie_data.withColumn("crew_size", F.size("credits.crew"))
        movie_data = movie_data.withColumn("cast_size", F.size("credits.cast"))

        movie_data = movie_data.withColumn(
            "cast",
            F.concat_ws(
                ", ",
                F.transform(F.col("credits.cast"), lambda x: x["name"])
            )
        )

        logger.info("Extracted director, cast, crew_size, cast_size.")
    except Exception as e:
        logger.error("Error extracting crew/cast info", exc_info=True)

    # -----------------------------
    # Step 4: Datatype conversion & scaling
    # -----------------------------
    try:
        movie_data = (
            movie_data
            .withColumn("budget", F.col("budget").cast("double"))
            .withColumn("revenue", F.col("revenue").cast("double"))
            .withColumn("popularity", F.col("popularity").cast("double"))
            .withColumn("vote_count", F.col("vote_count").cast("long"))
            .withColumn("vote_average", F.col("vote_average").cast("double"))
            .withColumn("runtime", F.col("runtime").cast("double"))
            .withColumn("release_date", F.to_date("release_date"))
            .withColumn("budget_musd", F.round(F.col("budget") / 1e6, 2))
            .withColumn("revenue_musd", F.round(F.col("revenue") / 1e6, 2))
        )
        logger.info("Converted datatypes and scaled budget/revenue.")
    except Exception as e:
        logger.error("Error converting datatypes", exc_info=True)

    # -----------------------------
    # Step 5: Handle unrealistic values
    # -----------------------------
    try:
        movie_data = (
            movie_data
            .withColumn("budget_musd", F.when(F.col("budget_musd") <= 0, None).otherwise(F.col("budget_musd")))
            .withColumn("revenue_musd", F.when(F.col("revenue_musd") <= 0, None).otherwise(F.col("revenue_musd")))
            .withColumn("runtime", F.when(F.col("runtime") <= 0, None).otherwise(F.col("runtime")))
        )

        for col in ["overview", "tagline"]:
            if col in movie_data.columns:
                movie_data = movie_data.withColumn(
                    col,
                    F.when(
                        (F.col(col).isNull()) | (F.col(col) == "") | (F.col(col) == "No Data"),
                        None
                    ).otherwise(F.col(col))
                )

        logger.info("Handled unrealistic values and placeholders.")
    except Exception as e:
        logger.error("Error handling unrealistic values", exc_info=True)

    # -----------------------------
    # Step 6: Remove duplicates & invalid rows
    # -----------------------------
    try:
        movie_data = (
            movie_data
            .dropDuplicates(["id"])
            .filter(F.col("id").isNotNull() & F.col("title").isNotNull())
        )
        logger.info(f"Row count after deduplication: {movie_data.count()}")
    except Exception as e:
        logger.error("Error removing duplicates", exc_info=True)

    # -----------------------------
    # Step 7: Keep rows with ≥10 non-null values
    # -----------------------------
    try:
        non_null_expr = sum(
            F.when(F.col(c).isNotNull(), 1).otherwise(0)
            for c in movie_data.columns
        )

        movie_data = (
            movie_data
            .withColumn("non_null_count", non_null_expr)
            .filter(F.col("non_null_count") >= 10)
            .drop("non_null_count")
        )

        logger.info(f"Row count after non-null filtering: {movie_data.count()}")
    except Exception as e:
        logger.error("Error filtering by non-null count", exc_info=True)

    # -----------------------------
    # Step 8: Filter released movies
    # -----------------------------
    try:
        movie_data = movie_data.filter(F.col("status") == "Released").drop("status")
        logger.info(f"Row count after Released filter: {movie_data.count()}")
    except Exception as e:
        logger.error("Error filtering Released movies", exc_info=True)

    # -----------------------------
    # Step 9: Reorder & select final columns
    # -----------------------------
    try:
        final_columns = [
            "id", "title", "tagline", "release_date", "genres",
            "belongs_to_collection", "original_language",
            "budget_musd", "revenue_musd",
            "production_companies", "production_countries",
            "vote_count", "vote_average", "popularity",
            "runtime", "overview", "spoken_languages",
            "poster_path", "cast", "cast_size",
            "director", "crew_size",
        ]

        movie_data = movie_data.select(
            [c for c in final_columns if c in movie_data.columns]
        )
        logger.info("Reordered and finalized columns.")
    except Exception as e:
        logger.error("Error reordering columns", exc_info=True)

    # -----------------------------
    # Step 10: Save cleaned data
    # -----------------------------
    output_path = "/data/cleaned_movie_data"

    try:
        movie_data.coalesce(1).write.mode("overwrite").parquet(output_path)
        logger.info(f"Cleaned data saved to {output_path}")
    except Exception as e:
        logger.error("Failed to save parquet data", exc_info=True)
        spark.stop()
        raise

    # -----------------------------
    # Stop Spark
    # -----------------------------
    spark.stop()
    logger.info("Spark session stopped.")
    logger.info("========== DATA PREPROCESSING COMPLETED ==========")

    return output_path


def main():
    filepath = preprocessing("data/movieData.json")


if __name__ == "__main__":
    main()