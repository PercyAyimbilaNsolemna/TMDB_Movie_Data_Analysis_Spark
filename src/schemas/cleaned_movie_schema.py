from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, IntegerType, DateType
)

def cleaned_movie_schema() -> StructType:
    return StructType([

        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("release_date", DateType(), True),
        StructField("genres", StringType(), True),
        StructField("belongs_to_collection", StringType(), True),
        StructField("original_language", StringType(), True),

        StructField("budget_musd", DoubleType(), True),
        StructField("revenue_musd", DoubleType(), True),

        StructField("production_companies", StringType(), True),
        StructField("production_countries", StringType(), True),

        StructField("vote_count", LongType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("runtime", DoubleType(), True),

        StructField("overview", StringType(), True),
        StructField("spoken_languages", StringType(), True),
        StructField("poster_path", StringType(), True),

        StructField("cast", StringType(), True),
        StructField("cast_size", IntegerType(), True),

        StructField("director", StringType(), True),
        StructField("crew_size", IntegerType(), True),
    ])
