import duckdb

# Create a connection
con = duckdb.connect()

# Install and load extensions
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# Set S3 region
con.execute("SET s3_region='us-east-1'")

# Create a table from S3 parquet file
con.execute("CREATE TABLE netflix AS SELECT * FROM read_parquet('s3://duckdb-md-dataset-121/netflix_daily_top_10.parquet')")

# Display the most popular TV Shows
tv_shows = con.execute("""
SELECT Title, max("Days In Top 10") as max_days_in_top_10
FROM netflix
WHERE Type='TV Show'
GROUP BY Title
ORDER BY max_days_in_top_10 DESC
LIMIT 5
""").fetchdf()

# Display the most popular Movies
movies = con.execute("""
SELECT Title, max("Days In Top 10") as max_days_in_top_10
FROM netflix
WHERE Type='Movie'
GROUP BY Title
ORDER BY max_days_in_top_10 DESC
LIMIT 5
""").fetchdf()

# Write the result to CSV
tv_shows.to_csv('output.csv', index=False)