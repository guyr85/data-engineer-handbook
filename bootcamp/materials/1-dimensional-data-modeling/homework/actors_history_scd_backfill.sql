WITH streak_started AS (
    SELECT
		actorid,
		actor,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> quality_class
        OR LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
		OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> is_active
		OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
               AS change_indicator
    FROM actors
)
, streak_identified AS (
	SELECT
		actorid,
		actor,
        current_year,
		quality_class,
		is_active,
        SUM(CASE WHEN change_indicator THEN 1 ELSE 0 END)
		OVER (PARTITION BY actorid ORDER BY current_year) as streak_identifier
     FROM streak_started
     )
, aggregated AS (
	SELECT
		actorid,
		actor,
		quality_class,
		is_active,
	    streak_identifier,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year
    FROM streak_identified
	GROUP BY 1,2,3,4,5
     )

SELECT actorid, actor, quality_class, is_active, start_year, end_year
FROM aggregated