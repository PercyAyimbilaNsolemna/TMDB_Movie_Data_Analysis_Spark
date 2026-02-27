"""
Spark schema definitions for raw TMDB API JSON data.

This module defines the StructType schema for raw data fetched directly from
The Movie Database API. The schema includes all nested structures returned by
the TMDB API including credits (cast/crew), genres, production information,
and other metadata.

The schema is used for validation during Spark data loading to ensure type
consistency and catch malformed records early in the pipeline.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, DoubleType, ArrayType, DateType
)


def raw_movie_schema() -> StructType:
    """
    Define the Spark schema for raw TMDB API movie data.

    Returns a StructType schema that matches the structure of JSON data
    returned by TMDB API /movie/{id} endpoint with append_to_response=credits.
    This includes full nested structures for credits, genres, production info, etc.

    Returns:
        pyspark.sql.types.StructType: Complete schema for raw movie data including:
            - Basic movie info: title, id, release_date, etc.
            - Nested credits: cast and crew arrays with member details
            - Financial data: budget, revenue, popularity
            - Metadata: genres, production companies/countries, languages
            - Ratings: vote_average, vote_count
            - Media: poster_path, backdrop_path, collections

    Note:
        All fields allow null values (nullable=True) to accommodate API inconsistencies.
        Some TMDB datasets have missing or incomplete information for certain fields.
        This schema should match the raw JSON structure exactly before preprocessing.
    """
    return StructType([

        StructField("adult", BooleanType(), True),
        StructField("backdrop_path", StringType(), True),

        StructField("belongs_to_collection", StructType([
            StructField("backdrop_path", StringType(), True),
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("poster_path", StringType(), True),
        ]), True),

        StructField("budget", LongType(), True),

        StructField("credits", StructType([
            StructField("cast", ArrayType(StructType([
                StructField("adult", BooleanType(), True),
                StructField("cast_id", LongType(), True),
                StructField("character", StringType(), True),
                StructField("credit_id", StringType(), True),
                StructField("gender", LongType(), True),
                StructField("id", LongType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("order", LongType(), True),
                StructField("original_name", StringType(), True),
                StructField("popularity", DoubleType(), True),
                StructField("profile_path", StringType(), True),
            ]), True)),

            StructField("crew", ArrayType(StructType([
                StructField("adult", BooleanType(), True),
                StructField("credit_id", StringType(), True),
                StructField("department", StringType(), True),
                StructField("gender", LongType(), True),
                StructField("id", LongType(), True),
                StructField("job", StringType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("original_name", StringType(), True),
                StructField("popularity", DoubleType(), True),
                StructField("profile_path", StringType(), True),
            ]), True)),
        ]), True),

        StructField("genres", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ])), True),

        StructField("homepage", StringType(), True),
        StructField("id", LongType(), True),
        StructField("imdb_id", StringType(), True),

        StructField("origin_country", ArrayType(StringType()), True),
        StructField("original_language", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("poster_path", StringType(), True),

        StructField("production_companies", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("logo_path", StringType(), True),
            StructField("name", StringType(), True),
            StructField("origin_country", StringType(), True),
        ])), True),

        StructField("production_countries", ArrayType(StructType([
            StructField("iso_3166_1", StringType(), True),
            StructField("name", StringType(), True),
        ])), True),

        StructField("release_date", StringType(), True),
        StructField("revenue", LongType(), True),
        StructField("runtime", LongType(), True),

        StructField("spoken_languages", ArrayType(StructType([
            StructField("english_name", StringType(), True),
            StructField("iso_639_1", StringType(), True),
            StructField("name", StringType(), True),
        ])), True),

        StructField("status", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("title", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", LongType(), True),
    ])
