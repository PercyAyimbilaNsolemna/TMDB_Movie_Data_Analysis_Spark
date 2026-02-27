"""
Spark schema definitions for cleaned and preprocessed movie data.

This module defines the StructType schema for processed movie data after the
10-step cleaning pipeline. The schema represents the final analysis-ready format
with flattened structures, scaled financial metrics, and enriched features.

All fields are nullable to handle cases where data was missing in the raw API
response, but the schema enforces consistent data types across the dataset.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, IntegerType, DateType
)


def cleaned_movie_schema() -> StructType:
    """
    Define the Spark schema for cleaned and processed movie data.

    Returns a StructType schema that represents the final output of the
    preprocessing pipeline. All nested structures have been flattened into
    pipe-separated strings, financial metrics have been scaled, and derived
    metrics (like crew/cast sizes) have been calculated.

    Returns:
        pyspark.sql.types.StructType: Schema for processed movie data including:
            - Core identifiers: id, title
            - Date/time: release_date (parsed to DateType)
            - Categorical: genres, collection, language, production info
            - Financial: budget_musd, revenue_musd (millions USD)
            - Metrics: vote_count, vote_average, popularity, runtime
            - Extracted content: cast, director, crew_size, cast_size
            - Descriptive: overview, tagline, languages, poster_path

    Note:
        - All text fields (genres, cast, director) are pipe-separated strings
        - Financial fields are in millions USD (e.g., 150 = $150 million)
        - release_date is converted to DateType for temporal analysis
        - cast_size and crew_size are IntegerType counts
        - All fields allow null values for missing data
    """
    return StructType([

        # Core identifiers (required for all records)
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("tagline", StringType(), True),
        
        # Temporal field (parsed to standard date format)
        StructField("release_date", DateType(), True),
        
        # Categorical fields (flattened to pipe-separated strings)
        StructField("genres", StringType(), True),
        StructField("belongs_to_collection", StringType(), True),
        StructField("original_language", StringType(), True),

        # Financial metrics (scaled to millions USD for readability)
        StructField("budget_musd", DoubleType(), True),
        StructField("revenue_musd", DoubleType(), True),

        # Production information (flattened strings)
        StructField("production_companies", StringType(), True),
        StructField("production_countries", StringType(), True),

        # Audience metrics
        StructField("vote_count", LongType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("popularity", DoubleType(), True),
        
        # Film properties
        StructField("runtime", DoubleType(), True),
        
        # Descriptive content
        StructField("overview", StringType(), True),
        StructField("spoken_languages", StringType(), True),
        StructField("poster_path", StringType(), True),

        # Cast and crew information
        StructField("cast", StringType(), True),
        StructField("cast_size", IntegerType(), True),

        StructField("director", StringType(), True),
        StructField("crew_size", IntegerType(), True),
    ])
