WITH last_year AS (
    SELECT * FROM actors
    WHERE current_year = 1970

), this_year AS (
     SELECT *, avg(rating) over (partition by actorid) as avg_rating FROM actor_films
     WHERE year = 1971
)
INSERT INTO actors
SELECT DISTINCT
        COALESCE(ly.actorid, ty.actorid) as actorid,
		COALESCE(ly.actor, ty.actor) as actor,
        COALESCE(ly.films, ARRAY[]::films[])
            || CASE WHEN ty.year IS NOT NULL THEN
			ARRAY_REMOVE(
            ARRAY_AGG(
				ROW(
					ty.filmid,
					ty.film,
					ty.votes,
					ty.rating,
					ty.year
					)::films
                )
            OVER (PARTITION BY ty.actorid ORDER BY ty.year),
            NULL
        ) end AS films,
         CASE
             WHEN ty.year IS NOT NULL THEN
                 (CASE WHEN ty.avg_rating > 8 THEN 'star'
                    WHEN ty.avg_rating > 7 THEN 'good'
                    WHEN ty.avg_rating > 6 THEN 'average'
                    ELSE 'bad' END)::quality_class
             ELSE ly.quality_class
         END as quality_class,
         ty.year IS NOT NULL as is_active,
         1971 AS current_season

    FROM last_year ly
    FULL OUTER JOIN this_year ty
    ON ly.actorid = ty.actorid
	