INSERT INTO actors
WITH years AS (
	SELECT *
    FROM GENERATE_SERIES(1970, 2021) AS year
),
actor_first_year AS (
	SELECT
        actorid,
		actor,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actorid, actor
),
actors_and_years AS (
    SELECT *
    FROM actor_first_year afy
    JOIN years y
        ON afy.first_year <= y.year
),
windowed AS (
    SELECT DISTINCT
        actors_and_years.actorid,
		actors_and_years.actor,
        actors_and_years.year,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN actor_films.year IS NOT NULL
                        THEN ROW(
							actor_films.filmid,
                            actor_films.film,
                            actor_films.votes,
                            actor_films.rating,
							actor_films.year
                        )::films
                END)
            OVER (PARTITION BY actors_and_years.actorid ORDER BY actors_and_years.year),
            NULL
        ) AS films
    FROM actors_and_years
    LEFT JOIN actor_films
        ON actors_and_years.actorid = actor_films.actorid
        AND actors_and_years.year = actor_films.year
    ORDER BY actors_and_years.actorid, actors_and_years.year
),
avg_rating AS (
	SELECT
		actorid,
		year,
		avg(rating) avg_rating
	FROM actor_films
	GROUP BY actorid, year
),
avg_rating_filed_years AS (
	SELECT
		actors_and_years.actorid,
		actors_and_years.year,
		avg_rating.avg_rating
	FROM actors_and_years
	LEFT JOIN avg_rating
    ON avg_rating.actorid = actors_and_years.actorid AND avg_rating.year = actors_and_years.year
),
final_avg_rating AS (
	SELECT
		actorid,
		year,
		ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN avg_rating IS NOT NULL
                        THEN avg_rating
                END)
            OVER (PARTITION BY actorid ORDER BY year),
			NULL
			) AS ratings
	FROM avg_rating_filed_years
)

SELECT
    w.actorid,
	w.actor,
    w.films,
    CASE
        WHEN ratings[CARDINALITY(ratings)] > 8 THEN 'star'
        WHEN ratings[CARDINALITY(ratings)] > 7 THEN 'good'
        WHEN ratings[CARDINALITY(ratings)] > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
	(films[CARDINALITY(films)]::films).year = w.year AS is_active,
    w.year AS current_year
FROM windowed w
left join final_avg_rating far
ON w.actorid = far.actorid AND w.year = far.year