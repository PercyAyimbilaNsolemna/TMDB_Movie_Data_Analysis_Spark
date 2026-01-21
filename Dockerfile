FROM jupyter/pyspark-notebook

# Set working directory
WORKDIR /TMDB_Movie_Data_Analysis_Spark

# Install extra Python libraries if needed later
# RUN pip install pandas numpy matplotlib
# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

EXPOSE 8888
