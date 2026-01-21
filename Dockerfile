FROM jupyter/pyspark-notebook

# Set working directory
WORKDIR /TMDB_Movie_Data_Analysis_Spark

# Install extra Python libraries if needed later
# RUN pip install pandas numpy matplotlib

EXPOSE 8888
