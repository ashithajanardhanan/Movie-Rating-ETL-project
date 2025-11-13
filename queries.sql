SELECT 
    m.title,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(r.rating) AS rating_count
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 1;



SELECT 
    g.name AS genre,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(DISTINCT m.movie_id) AS num_movies
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN movies m ON mg.movie_id = m.movie_id
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY g.name
ORDER BY avg_rating DESC
LIMIT 5;


SELECT 
    director,
    COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;


SELECT 
    m.release_year,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(DISTINCT m.movie_id) AS num_movies
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;
