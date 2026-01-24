#!/usr/bin/env python3
# data_cleaning_spark.py

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------
# Setup logging
# -----------------------------
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "data_cleaning.log")

logging.basicConfig(
    filename=log_file,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger()

# -----------------------------
# Initialize Spark
# -----------------------------
try:
    spark = SparkSession.builder \
        .appName("TMDB Movie Data Cleaning") \
        .getOrCreate()
    logger.info("Spark session started successfully.")
except Exception as e:
    logger.error(f"Failed to start Spark session: {e}")
    sys.exit(1)

# -----------------------------
# Load raw data
# -----------------------------
try:
    input_path = "../data/movieData.json"
    movie_data = spark.read.json(input_path)
    logger.info(f"Data loaded successfully from {input_path}")
    logger.info(f"Initial row count: {movie_data.count()}")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    sys.exit(1)

# -----------------------------
# Step 1: Drop irrelevant columns
# -----------------------------
try:
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    movie_data = movie_data.drop(*[c for c in cols_to_drop if c in movie_data.columns])
    logger.info(f"Dropped irrelevant columns: {cols_to_drop}")
except Exception as e:
    logger.error(f"Error dropping columns: {e}")

# -----------------------------
# Step 2: Parse JSON-like array columns using Spark SQL functions
# -----------------------------
try:
    array_columns = {
        "genres": "name",
        "production_countries": "name",
        "production_companies": "name",
        "spoken_languages": "english_name"
    }

    for col, key in array_columns.items():
        if col in movie_data.columns:
            movie_data = movie_data.withColumn(
                col,
                F.concat_ws(
                    "|",
                    F.expr(f"transform({col}, x -> x.{key})")
                )
            )
    logger.info("Parsed JSON-like array columns into pipe-separated strings using Spark SQL functions.")
except Exception as e:
    logger.error(f"Error parsing JSON columns: {e}")

# -----------------------------
# Step 3: Extract directors and calculate crew/cast size
# -----------------------------
try:
    # Directors
    movie_data = movie_data.withColumn(
        "director",
        F.expr("concat_ws(', ', transform(filter(credits.crew, x -> x.job = 'Director'), x -> x.name))")
    )

    # Crew size
    movie_data = movie_data.withColumn(
        "crew_size",
        F.size("credits.crew")
    )

    # Cast size
    movie_data = movie_data.withColumn(
        "cast_size",
        F.size("credits.cast")
    )
    logger.info("Extracted directors and calculated crew/cast sizes.")
except Exception as e:
    logger.error(f"Error extracting directors or calculating sizes: {e}")

# -----------------------------
# Step 4: Convert datatypes
# -----------------------------
try:
    movie_data = movie_data.withColumn("budget", F.col("budget").cast("double")) \
                           .withColumn("revenue", F.col("revenue").cast("double")) \
                           .withColumn("popularity", F.col("popularity").cast("double")) \
                           .withColumn("vote_count", F.col("vote_count").cast("long")) \
                           .withColumn("vote_average", F.col("vote_average").cast("double")) \
                           .withColumn("runtime", F.col("runtime").cast("double")) \
                           .withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd")) \
                           .withColumn("budget_musd", F.round(F.col("budget") / 1e6, 2)) \
                           .withColumn("revenue_musd", F.round(F.col("revenue") / 1e6, 2))
    logger.info("Converted datatypes and scaled budget/revenue to million USD.")
except Exception as e:
    logger.error(f"Error converting datatypes: {e}")

# -----------------------------
# Step 5: Handle unrealistic values and placeholders
# -----------------------------
try:
    movie_data = movie_data.withColumn(
        "budget_musd", F.when(F.col("budget_musd") <= 0, None).otherwise(F.col("budget_musd"))
    ).withColumn(
        "revenue_musd", F.when(F.col("revenue_musd") <= 0, None).otherwise(F.col("revenue_musd"))
    ).withColumn(
        "runtime", F.when(F.col("runtime") <= 0, None).otherwise(F.col("runtime"))
    )

    for col in ["overview", "tagline"]:
        if col in movie_data.columns:
            movie_data = movie_data.withColumn(
                col,
                F.when((F.col(col).isNull()) | (F.col(col) == "No Data") | (F.col(col) == ""), None)
                .otherwise(F.col(col))
            )
    logger.info("Handled unrealistic values and placeholder text.")
except Exception as e:
    logger.error(f"Error handling unrealistic values: {e}")

# -----------------------------
# Step 6: Remove duplicates & invalid rows
# -----------------------------
try:
    movie_data = movie_data.dropDuplicates(["id"]).filter(F.col("id").isNotNull() & F.col("title").isNotNull())
    logger.info("Removed duplicates and rows with null id or title.")
    logger.info(f"Row count after duplicates removal: {movie_data.count()}")
except Exception as e:
    logger.error(f"Error removing duplicates or invalid rows: {e}")

# -----------------------------
# Step 7: Keep only rows with at least 10 non-null columns
# -----------------------------
try:
    non_null_count = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in movie_data.columns)
    movie_data = movie_data.withColumn("non_null_count", non_null_count).filter(F.col("non_null_count") >= 10)
    movie_data = movie_data.drop("non_null_count")
    logger.info("Kept only rows with at least 10 non-null columns.")
    logger.info(f"Row count after non-null filtering: {movie_data.count()}")
except Exception as e:
    logger.error(f"Error filtering rows with insufficient non-null values: {e}")

# -----------------------------
# Step 8: Filter released movies
# -----------------------------
try:
    movie_data = movie_data.filter(F.col("status") == "Released").drop("status")
    logger.info("Filtered to only include 'Released' movies.")
    logger.info(f"Row count after filtering released movies: {movie_data.count()}")
except Exception as e:
    logger.error(f"Error filtering released movies: {e}")

# -----------------------------
# Step 9: Reorder columns and drop extras
# -----------------------------
try:
    final_columns = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
        'production_countries', 'vote_count', 'vote_average', 'popularity',
        'runtime', 'overview', 'spoken_languages', 'poster_path',
        'cast', 'cast_size', 'director', 'crew_size'
    ]
    movie_data = movie_data.select([c for c in final_columns if c in movie_data.columns])
    logger.info("Reordered columns and dropped extras.")
except Exception as e:
    logger.error(f"Error reordering columns: {e}")

# -----------------------------
# Step 10: Save cleaned data as Parquet
# -----------------------------
try:
    output_file = "../data/movieData_cleaned"
    movie_data.write.parquet(output_file)
    logger.info(f"Cleaned data saved successfully to {output_file}")
except Exception as e:
    logger.error(f"Failed to save cleaned data: {e}")

# -----------------------------
# Stop Spark
# -----------------------------
spark.stop()
logger.info("Spark session stopped. Data cleaning process completed.")
