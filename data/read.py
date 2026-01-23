import csv
import sys

# Allow very large CSV fields
csv.field_size_limit(sys.maxsize)

CSV_FILE = "C:/Users/AyimbilaNsolemnaPerc/Desktop/LABS/DEM05/TMDB_Movie_Data_Analysis_Spark/data/movieData.csv"
MAX_CHARS = 120   # truncate long fields

with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    row = next(reader)   # first data row

print("\n=== FIRST ROW (CLEAN PREVIEW) ===\n")

for key, value in row.items():
    value = value.strip()

    if len(value) > MAX_CHARS:
        preview = value[:MAX_CHARS] + "..."
    else:
        preview = value

    print(f"{key}:")
    print(f"  {preview}")
    print("-" * 60)


