import os
import time
import re
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------- CONFIG ----------
MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
DB_URL = "mysql+pymysql://root:ashitha@localhost/moviesdb"
OMDB_API_KEY = os.environ.get("OMDB_API_KEY")
OMDB_URL = "http://www.omdbapi.com/"


def parse_title_and_year(title):
    """Return (title_without_year, year_or_None)."""
    if not isinstance(title, str):
        return title, None
    m = re.match(r'^(.*)\s+\((\d{4})\)\s*$', title.strip())
    if m:
        return m.group(1).strip(), m.group(2)
    return title.strip(), None


def call_omdb(title, year=None, timeout=8):
    """Call OMDb safely and return JSON or None."""
    if not OMDB_API_KEY:
        return None
    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = year
    try:
        r = requests.get(OMDB_URL, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if data.get("Response") == "True":
            return data
    except Exception as e:
        print(f"⚠ OMDb error for '{title}' ({year}): {e}")
    return None


# ---------- SCHEMA ----------
def init_schema(engine):
    """Initialize MySQL schema from external schema.sql file."""
    schema_file = "schema.sql"

    # Check if schema.sql exists in the same directory
    if not os.path.exists(schema_file):
        print("schema.sql not found. Please make sure it's in the same folder as etl.py.")
        return

    print(f"Loading schema from {schema_file}...")

    # Read and execute all statements from schema.sql
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    # Split on semicolons to execute each statement separately
    statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                print(f"Error executing statement:\n{stmt}\nError: {e}")
    print("Schema created successfully from schema.sql.")



# ---------- ETL STEPS ----------
def load_csvs():
    print("Loading CSVs...")
    movies = pd.read_csv(MOVIES_CSV).head(100)
    ratings = pd.read_csv(RATINGS_CSV)

    movie_ids = set(movies["movieId"])
    ratings = ratings[ratings["movieId"].isin(movie_ids)]

    print(f"Movies rows: {len(movies)},  Ratings rows: {len(ratings)}")
    return movies, ratings


def upsert_movies(engine, movies_df, enrich_with_omdb=True, throttle=0.25):
    """Upsert movies into DB. Optionally enrich via OMDb for missing details."""
    print("Preparing movies for upsert...")
    df = movies_df.rename(columns={"movieId": "movie_id", "title": "title", "genres": "genres"}).copy()
    df["parsed_title"], df["parsed_year"] = zip(*df["title"].apply(parse_title_and_year))

    # get existing movie_ids so we skip already-present movies
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT movie_id FROM movies")).fetchall()
    existing_ids = set(r[0] for r in existing)

    to_insert = []
    for _, row in df.iterrows():
        mid = int(row.movie_id)
        if mid in existing_ids:
            continue  # skip already present
        title = row.parsed_title
        year = row.parsed_year

        base = {
            "movie_id": mid,
            "title": row.title,
            "release_year": int(year) if year else None,
            "imdb_id": None,
            "director": None,
            "plot": None,
            "box_office": None,
            "runtime": None,
            "imdb_rating": None
        }

        if enrich_with_omdb and OMDB_API_KEY:
            print(f"Fetching from OMDb: {title} ({year})")
            om = call_omdb(title, year)
            time.sleep(throttle)
            if om:
                base["imdb_id"] = om.get("imdbID")
                base["director"] = om.get("Director") if om.get("Director") != "N/A" else None
                base["plot"] = om.get("Plot") if om.get("Plot") != "N/A" else None
                base["box_office"] = om.get("BoxOffice") if om.get("BoxOffice") != "N/A" else None
                base["runtime"] = om.get("Runtime") if om.get("Runtime") != "N/A" else None
                try:
                    base["imdb_rating"] = float(om.get("imdbRating")) if om.get("imdbRating") and om.get("imdbRating") != "N/A" else None
                except:
                    base["imdb_rating"] = None
                try:
                    if om.get("Year") and om.get("Year").isdigit():
                        base["release_year"] = int(om.get("Year"))
                except:
                    pass

        to_insert.append(base)

    print(f"{len(to_insert)} new movies to insert (after skipping existing).")

    if to_insert:
        insert_sql = text("""
            INSERT INTO movies (movie_id, title, release_year, imdb_id, director, plot, box_office, runtime, imdb_rating)
            VALUES (:movie_id, :title, :release_year, :imdb_id, :director, :plot, :box_office, :runtime, :imdb_rating)
            ON DUPLICATE KEY UPDATE
              title = VALUES(title),
              release_year = VALUES(release_year),
              imdb_id = COALESCE(VALUES(imdb_id), movies.imdb_id),
              director = COALESCE(VALUES(director), movies.director),
              plot = COALESCE(VALUES(plot), movies.plot),
              box_office = COALESCE(VALUES(box_office), movies.box_office),
              runtime = COALESCE(VALUES(runtime), movies.runtime),
              imdb_rating = COALESCE(VALUES(imdb_rating), movies.imdb_rating),
              last_updated = CURRENT_TIMESTAMP
        """)
        # insert in chunks
        chunk_size = 500
        with engine.begin() as conn:
            for i in range(0, len(to_insert), chunk_size):
                chunk = to_insert[i:i + chunk_size]
                try:
                    conn.execute(insert_sql, chunk)
                except SQLAlchemyError as e:
                    print("⚠ Error inserting movie chunk:", e)
    print("Movies upsert complete.")


def normalize_and_load_genres(engine, movies_df):
    """Normalize genres and populate genres and movie_genres tables."""
    print("Normalizing & loading genres...")
    df = movies_df.rename(columns={"movieId": "movie_id", "genres": "genres"}) if "movieId" in movies_df.columns else movies_df.copy()
    # Collect unique genre names
    all_genres = set()
    for g in df["genres"].dropna().astype(str):
        for item in g.split("|"):
            name = item.strip()
            if name and name != "(no genres listed)":
                all_genres.add(name)
    all_genres = sorted(all_genres)

    with engine.begin() as conn:
        for g in all_genres:
            try:
                conn.execute(text("INSERT IGNORE INTO genres (name) VALUES (:name)"), {"name": g})
            except SQLAlchemyError:
                pass
        res = conn.execute(text("SELECT genre_id, name FROM genres")).fetchall()
    genre_map = {r[1]: r[0] for r in res}

    mappings = []
    for _, row in df.iterrows():
        mid = int(row["movie_id"]) if "movie_id" in row else int(row["movieId"])
        gen_str = row.get("genres")
        if not gen_str or pd.isna(gen_str):
            continue
        for g in gen_str.split("|"):
            gn = g.strip()
            if gn and gn in genre_map:
                mappings.append({"movie_id": mid, "genre_id": genre_map[gn]})

    with engine.begin() as conn:
        for m in mappings:
            try:
                conn.execute(text("INSERT IGNORE INTO movie_genres (movie_id, genre_id) VALUES (:movie_id, :genre_id)"), m)
            except SQLAlchemyError:
                pass

    print(f"Loaded genres ({len(all_genres)}) and movie_genres mappings ({len(mappings)}).")


def load_ratings(engine, ratings_df):
    """Load ratings with INSERT IGNORE semantics to avoid duplicate PK errors."""
    print("Loading ratings...")
    df = ratings_df.rename(columns={"userId": "user_id", "movieId": "movie_id", "rating": "rating", "timestamp": "ts"}).copy()
    rows = df.to_dict(orient="records")
    insert_sql = text("INSERT IGNORE INTO ratings (user_id, movie_id, rating, ts) VALUES (:user_id, :movie_id, :rating, :ts)")
    chunk_size = 1000
    with engine.begin() as conn:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            try:
                conn.execute(insert_sql, chunk)
            except SQLAlchemyError as e:
                print("Error inserting rating chunk:", e)
    print("Ratings loaded.")


# ---------- MAIN ----------
def main():
    print("Starting ETL process...")
    engine = create_engine(DB_URL)
    print("Connected to DB.")

    init_schema(engine)
    movies, ratings = load_csvs()

    upsert_movies(engine, movies, enrich_with_omdb=bool(OMDB_API_KEY), throttle=0.2)
    if "movieId" in movies.columns:
        movies_for_genre = movies.rename(columns={"movieId": "movie_id"})
    else:
        movies_for_genre = movies
    normalize_and_load_genres(engine, movies_for_genre)
    load_ratings(engine, ratings)

    print("ETL complete.")


if __name__ == "__main__":
    main()
