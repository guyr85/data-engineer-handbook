with game_details_dedup_rows AS (
-- The key game_details table is: game_id, team_id, player_id
-- There are more than one row(duplicate) for the key. I'll dedup with row_num() window function
-- Could use SELECT DISTINCT ON pattern, which can sometimes be more efficient for deduplication in certain SQL engines.
	SELECT *,
		   row_number() over (partition by game_id, team_id, player_id) AS rn
	FROM public.game_details
)
-- Query and fiter on rn=1. Will keep one row for each key of the table.
SELECT *
FROM game_details_dedup_rows
WHERE rn = 1