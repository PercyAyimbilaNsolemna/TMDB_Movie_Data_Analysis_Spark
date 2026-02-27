#!/usr/bin/env python3
"""
TMDB movie data preprocessing and cleaning module.

This module implements a comprehensive data cleaning pipeline for raw TMDB
API data using Apache Spark. It transforms unstructured nested JSON data into a
clean, analysis-ready parquet format.

The preprocessing pipeline:
1. Drops irrelevant columns not needed for analysis
2. Flattens array<struct> columns into pipe-separated strings
3. Extracts director, cast, and crew information from nested credits
4. Converts data types and scales budget/revenue to millions USD
5. Handles unrealistic values (negatives, placeholders)
6. Removes duplicates and rows missing required fields
7. Filters out movies with insufficient non-null values
8. Keeps only officially released movies
9. Reorders columns for output consistency
10. Saves cleaned data as Parquet files

Key Features:
    - Comprehensive error handling and logging at each step
    - Prevents data loss through defensive column operations
    - Data quality metrics logged throughout pipeline
    - Output schema validated against cleaned_movie_schema

Module Functions:
    preprocessing: Execute the full 10-step cleaning pipeline
    main: Entry point for command-line execution
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.utils.logger import setup_logger
from src.schemas.raw_data_schema import raw_movie_schema
from src.schemas.cleaned_movie_schema import cleaned_movie_schema


def preprocessing(spark, filepath="../data/raw_data/movieData.json"):
    """
    Clean and preprocess raw TMDB movie data using Apache Spark.

    Executes a 10-step data cleaning pipeline transforming raw nested JSON data
    into a clean, analysis-ready Parquet dataset. Each step includes error handling
    and logging for transparency and debugging.

    The pipeline handles:
        - Schema validation and type conversion
        - Nested data flattening (arrays and structs)
        - Data quality checks (nullness, realistic value ranges)
        - Duplicate removal and record filtering
        - Column reordering for standardized output

    Args:
        spark (pyspark.sql.SparkSession): Active Spark session for data processing.
        filepath (str): Path to raw NDJSON movie data file.
                       Defaults to "/data/raw_data/movieData.json".
                       Expected format: One JSON object per line.

    Returns:
        str: Path to output directory containing cleaned parquet files.
            Typically "/data/cleaned_movie_data".

    Raises:
        Exception: Re-raises exceptions during critical steps (data load, parquet write)
                  after logging. Non-critical step failures are logged but don't halt pipeline.

    Example:
        >>> spark = SparkSession.builder.appName("MovieCleaning").getOrCreate()
        >>> cleaned_path = preprocessing(spark)
        >>> print(f"Cleaned data saved to: {cleaned_path}")
        Cleaned data saved to: /data/cleaned_movie_data
    """

    # Initialize logger for detailed preprocessing operation tracking
    logger = setup_logger(
        name="preprocessing",
        log_file="/logs/data_cleaning.log"
    )

    logger.info("========== STARTING DATA PREPROCESSING ==========")

    # =================================================================
    # Step 0: Load raw data with schema validation
    # =================================================================
    try:
        # Load JSON data with predefined schema to catch type inconsistencies early
        # Using schema validation prevents costly type inference from large datasets
        movie_data = (
            spark.read
            .schema(raw_movie_schema())  # Apply predefined schema
            .json(filepath)
        )
        logger.info(f"Data loaded from {filepath}")
        logger.info(f"Initial row count: {movie_data.count()}")
    except Exception as e:
        logger.error(f"Failed to load data: {e}", exc_info=True)
        spark.stop()
        raise  # Critical failure - cannot proceed without data

    # =================================================================
    # Step 1: Drop irrelevant columns
    # =================================================================
    try:
        # Remove columns not needed for analysis
        # Reduces dataset size and processing time
        cols_to_drop = [
            "adult",              # Rating classification not useful for this analysis
            "backdrop_path",      # Image paths not needed for data analysis
            "origin_country",     # Similar info in production_countries
            "original_title",     # We use the localized title
            "imdb_id",           # Not part of TMDB primary analysis
            "video",             # Distribution channel not primary metric
            "homepage",          # External link not useful for analysis
        ]
        
        # Only drop columns that actually exist in the dataset
        movie_data = movie_data.drop(
            *[c for c in cols_to_drop if c in movie_data.columns]
        )
        logger.info(f"Dropped irrelevant columns: {cols_to_drop}")
    except Exception as e:
        logger.warning(f"Column drop issue: {e}", exc_info=True)

    # =================================================================
    # Step 2: Parse array columns into pipe-separated strings
    # =================================================================
    try:
        # Transform nested array<struct> columns into flattened strings
        # Enables easier analysis and visualization downstream
        array_columns = {
            "genres": "name",                      # Extract genre names
            "production_countries": "name",        # Extract country names
            "production_companies": "name",        # Extract company names
            "spoken_languages": "english_name",    # Extract language names (English)
        }

        for col, key in array_columns.items():
            if col in movie_data.columns:
                # Transform: [struct1, struct2, ...] -> "value1|value2|..."
                movie_data = movie_data.withColumn(
                    col,
                    F.concat_ws("|", F.expr(f"transform({col}, x -> x.{key})"))
                )

        logger.info("Parsed array-based columns successfully.")
    except Exception as e:
        logger.error("Error parsing array columns", exc_info=True)

    # =================================================================
    # Step 3: Extract collection, director, cast & crew sizes
    # =================================================================
    try:
        # Extract collection name from nested struct
        movie_data = movie_data.withColumn(
            "belongs_to_collection",
            F.col("belongs_to_collection.name")
        )

        # Extract director names from credits.crew (filter by job = 'Director')
        # Filters to only crew members with job title 'Director', then concatenates names
        movie_data = movie_data.withColumn(
            "director",
            F.expr(
                "concat_ws(', ', transform(filter(credits.crew, x -> x.job = 'Director'), x -> x.name))"
            )
        )

        # Calculate crew and cast sizes for metrics
        # Useful for analyzing relationship between squad size and performance
        movie_data = movie_data.withColumn("crew_size", F.size("credits.crew"))
        movie_data = movie_data.withColumn("cast_size", F.size("credits.cast"))

        # Extract cast names from credits.cast array
        # Concatenates all actor names with comma separator
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

    # =================================================================
    # Step 4: Data type conversion and financial scaling
    # =================================================================
    try:
        # Convert data types to appropriate Spark types
        # Also divide budget and revenue by 1 million for readability (e.g., 150 = $150M)
        movie_data = (
            movie_data
            .withColumn("budget", F.col("budget").cast("double"))
            .withColumn("revenue", F.col("revenue").cast("double"))
            .withColumn("popularity", F.col("popularity").cast("double"))
            .withColumn("vote_count", F.col("vote_count").cast("long"))
            .withColumn("vote_average", F.col("vote_average").cast("double"))
            .withColumn("runtime", F.col("runtime").cast("double"))
            .withColumn("release_date", F.to_date("release_date"))  # Parse date string
            # Create scaled versions in millions USD for easier interpretation
            .withColumn("budget_musd", F.round(F.col("budget") / 1e6, 2))
            .withColumn("revenue_musd", F.round(F.col("revenue") / 1e6, 2))
        )
        logger.info("Converted datatypes and scaled budget/revenue.")
    except Exception as e:
        logger.error("Error converting datatypes", exc_info=True)

    # =================================================================
    # Step 5: Handle unrealistic/missing values
    # =================================================================
    try:
        # Remove non-positive financial values (0 or negative = missing data from API)
        # In TMDB API, 0 budget/revenue indicates the data wasn't available
        movie_data = (
            movie_data
            .withColumn("budget_musd", F.when(F.col("budget_musd") <= 0, None).otherwise(F.col("budget_musd")))
            .withColumn("revenue_musd", F.when(F.col("revenue_musd") <= 0, None).otherwise(F.col("revenue_musd")))
            .withColumn("runtime", F.when(F.col("runtime") <= 0, None).otherwise(F.col("runtime")))
        )

        # Standardize text fields: treat empty strings and placeholder text as null
        # Makes analysis cleaner by treating all missing values consistently
        for col in ["overview", "tagline"]:
            if col in movie_data.columns:
                movie_data = movie_data.withColumn(
                    col,
                    F.when(
                        (F.col(col).isNull()) | (F.col(col) == "") | (F.col(col) == "No Data"),
                        None  # Treat all missing indicators as null
                    ).otherwise(F.col(col))
                )

        logger.info("Handled unrealistic values and placeholders.")
    except Exception as e:
        logger.error("Error handling unrealistic values", exc_info=True)

    # =================================================================
    # Step 6: Remove duplicates and invalid rows
    # =================================================================
    try:
        # Keep first occurrence of each movie (by id) and filter out records without id/title
        # Invalid records can't be properly identified or analyzed
        movie_data = (
            movie_data
            .dropDuplicates(["id"])  # Remove duplicate movie records
            .filter(F.col("id").isNotNull() & F.col("title").isNotNull())  # Keep only valid records
        )
        logger.info(f"Row count after deduplication: {movie_data.count()}")
    except Exception as e:
        logger.error("Error removing duplicates", exc_info=True)

    # =================================================================
    # Step 7: Data quality filtering - keep rows with sufficient non-null values
    # =================================================================
    try:
        # Create expression that counts non-null values across all columns
        # This is a data quality check: movies with very sparse data aren't useful
        non_null_expr = sum(
            F.when(F.col(c).isNotNull(), 1).otherwise(0)
            for c in movie_data.columns
        )

        # Keep only rows with at least 10 non-null values out of all columns
        # This threshold balances data completeness with dataset size
        movie_data = (
            movie_data
            .withColumn("non_null_count", non_null_expr)
            .filter(F.col("non_null_count") >= 10)  # Quality threshold
            .drop("non_null_count")  # Drop temporary column
        )

        logger.info(f"Row count after non-null filtering: {movie_data.count()}")
    except Exception as e:
        logger.error("Error filtering by non-null count", exc_info=True)

    # =================================================================
    # Step 8: Filter for officially released movies only
    # =================================================================
    try:
        # Keep only movies with status == "Released"
        # Excludes planned, in production, rumored, and other pre-release movies
        # These aren't suitable for revenue/performance analysis
        movie_data = movie_data.filter(F.col("status") == "Released").drop("status")
        logger.info(f"Row count after Released filter: {movie_data.count()}")
    except Exception as e:
        logger.error("Error filtering Released movies", exc_info=True)

    # =================================================================
    # Step 9: Reorder and select final columns for output schema
    # =================================================================
    try:
        # Define the standard column order for output
        # This ensures consistency across pipeline runs
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

        # Select only columns that exist, and in the defined order
        movie_data = movie_data.select(
            [c for c in final_columns if c in movie_data.columns]
        )
        logger.info("Reordered and finalized columns.")
    except Exception as e:
        logger.error("Error reordering columns", exc_info=True)

    # =================================================================
    # Step 10: Write cleaned data as Parquet files
    # =================================================================
    output_path = "/data/cleaned_movie_data"

    try:
        # Write data matching the cleaned_movie_schema for consistency
        # Parquet format: compressed, columnar, enables efficient queries
        (
            movie_data
            .select([f.name for f in cleaned_movie_schema().fields])
            .write
            .mode("overwrite")  # Replace any existing output
            .parquet(output_path)
        )

        logger.info(f"Cleaned data saved to {output_path}")
    except Exception as e:
        logger.error("Failed to save parquet data", exc_info=True)
        spark.stop()
        raise  # Critical failure - can't proceed without saving results

    # =================================================================
    # Pipeline complete
    # =================================================================
    logger.info("========== DATA PREPROCESSING COMPLETED ==========")

    return output_path


def main():
    """
    Main entry point for preprocessing module.

    Initializes Spark session and executes the preprocessing pipeline.
    This function is called when the module is run directly as a script.
    """
    # Note: In actual deployment, SparkSession should be passed from caller
    # For development/testing, create a session here if needed
    filepath = preprocessing("data/movieData.json")


if __name__ == "__main__":
    main()