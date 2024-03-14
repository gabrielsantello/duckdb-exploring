import duckdb
import csv

# Create a connection
con = duckdb.connect()

# Install and load extensions
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# Set S3 region
con.execute("SET s3_region='us-east-1'")

# Create a table from S3 parquet file
con.execute("CREATE TABLE netflix AS SELECT * FROM read_parquet('s3://duckdb-md-dataset-121/netflix_daily_top_10.parquet')")

# Query to get the most popular TV Shows
query = """
SELECT Title, max("Days In Top 10") as max_days_in_top_10
FROM netflix
WHERE Type='TV Show'
GROUP BY Title
ORDER BY max_days_in_top_10 DESC
LIMIT 5
"""

# Execute the query and fetch the result
result = con.execute(query).fetchall()

# Write the result to a CSV file
with open('output.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Title", "Max Days In Top 10"])  # Write header
    writer.writerows(result)  # Write data