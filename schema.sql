CREATE TABLE IF NOT EXISTS movies (
  movie_id INT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  release_year INT NULL,
  imdb_id VARCHAR(50) NULL,
  director VARCHAR(255) NULL,
  plot TEXT NULL,
  box_office VARCHAR(50) NULL,
  runtime VARCHAR(50) NULL,
  imdb_rating FLOAT NULL,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS genres (
  genre_id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
  movie_id INT NOT NULL,
  genre_id INT NOT NULL,
  PRIMARY KEY (movie_id, genre_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
  FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ratings (
  user_id INT NOT NULL,
  movie_id INT NOT NULL,
  rating FLOAT NOT NULL,
  ts BIGINT,
  PRIMARY KEY (user_id, movie_id, ts),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
);
