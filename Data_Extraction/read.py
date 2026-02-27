"""
Data preview utility for TMDB movie dataset.

This module reads the first row from a CSV file containing TMDB movie data
and displays a formatted preview of the raw data. Useful for understanding the
structure and content of the dataset before processing.

The preview truncates long fields to improve readability in console output.
"""

import csv
import sys

# Configure CSV reader to handle very large field values
# (Important for fields like overviews, plot summaries, and cast information)
csv.field_size_limit(sys.maxsize)

# Path to the raw TMDB movie data CSV file
CSV_FILE = "../data/raw_data/movieData.json"

# Maximum character limit for field display in console preview
# Longer values will be truncated with "..." for readability
MAX_CHARS = 120

# Read the CSV file and retrieve the first data row
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    row = next(reader)   # Extract the first data row

# Display formatted header
print("\n=== FIRST ROW (CLEAN PREVIEW) ===\n")

# Iterate through each column and display preview
for key, value in row.items():
    if value is not None:
        # Strip leading/trailing whitespace from field value
        value = value.strip()

        # Truncate long values and add ellipsis to indicate truncation
        if len(value) > MAX_CHARS:
            preview = value[:MAX_CHARS] + "..."
        else:
            preview = value

        # Display the field name and preview value
        print(f"{key}:")
        print(f"  {preview}")
        print("-" * 60)


