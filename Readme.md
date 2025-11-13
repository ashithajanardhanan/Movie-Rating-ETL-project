Overview

This project implements an ETL (Extract, Transform, Load) pipeline that extracts movie and rating data from CSV files, enriches them using the OMDb API, and loads the results into a MySQL database for analysis.

The objective is to demonstrate a practical data engineering workflow from data ingestion and transformation to enrichment and analytical querying.

---

Setup Instructions

Prerequisites
- Python 3.10 or above
- MySQL 8.0 or above
- OMDb API key (can be obtained from https://www.omdbapi.com/apikey.aspx)

Install dependencies:
pip install -r requirements.txt


Configure the OMDb API key:
set OMDB_API_KEY=api_key

Run the ETL pipeline:
python etl.py

--------------------------------------------------

Design Choices and Assumptions

1. The database schema follows a normalized design with separate tables for movies, genres, and ratings.

2. A bridge table (movie_genres) handles the many-to-many relationship between movies and genres.

3. Only the first 100 movies from the dataset are enriched using the OMDb API to respect free-tier API rate limits.

4. The ETL process is idempotent. Running it multiple times does not create duplicate records.

5. API requests are rate-limited using a short delay between calls to avoid throttling.

6. Missing or invalid API data (e.g., “N/A” values) are stored as NULL for consistency.

-----------------------------------------------------------


Challenges and Solution

OMDb API rate limits: Restricted enrichment to 100 movies and added delays between API calls.

Duplicate data during re-runs: Used MySQL features like INSERT IGNORE and ON DUPLICATE KEY UPDATE.

