CREATE TABLE game_details_dashboard AS
WITH game_details_augmented AS (
	-- I use distinct to remove duplicates
	SELECT DISTINCT CAST(gd.game_id AS TEXT) game_id,
	       CAST(games.season AS TEXT) season,
		   CASE WHEN gd.team_id = games.team_id_home AND home_team_wins = 1 THEN 1
				WHEN gd.team_id = games.team_id_away AND home_team_wins = 0 THEN 1
				ELSE 0
		   END AS team_won,
		   COALESCE(team_abbreviation, 'unknown') AS team_abbreviation,
           COALESCE(player_name, 'unknown')  AS player_name,
		   pts
    FROM game_details AS gd
	LEFT JOIN games AS games
		ON gd.game_id = games.game_id
)

SELECT
       -- Creating all the aggregation_level combinations
	   CASE
		   WHEN GROUPING(team_abbreviation) = 0 AND GROUPING(player_name) = 0 THEN 'player_name__team_abbreviation'
           WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player_name__season'
		   WHEN GROUPING(team_abbreviation) = 0 THEN 'team_abbreviation'
		   ELSE 'overall'
       END as aggregation_level,
       COALESCE(team_abbreviation, '(overall)') as team_abbreviation,
       COALESCE(player_name, '(overall)') as player_name,
	   COALESCE(season, '(overall)') as season,
       COUNT(1) as number_of_games,
	   sum(pts) as pts,
	   COUNT(DISTINCT CASE WHEN team_won = 1 THEN game_id END) AS team_won
FROM game_details_augmented
GROUP BY GROUPING SETS (
	(player_name, team_abbreviation),
	(player_name, season),
	(team_abbreviation)
)
ORDER BY COUNT(1) DESC;

-- who scored the most points playing for one team?
SELECT team_abbreviation, player_name, pts
FROM game_details_dashboard
WHERE aggregation_level = 'player_name__team_abbreviation'
AND pts = (select max(pts) from game_details_dashboard WHERE aggregation_level = 'player_name__team_abbreviation')

-- who scored the most points in one season?
SELECT player_name, season, pts
FROM game_details_dashboard
WHERE aggregation_level = 'player_name__season'
AND pts = (select max(pts) from game_details_dashboard WHERE aggregation_level = 'player_name__season')

-- which team has won the most games?
SELECT team_abbreviation, team_won
FROM game_details_dashboard
WHERE aggregation_level = 'team_abbreviation'
ORDER BY team_won DESC
LIMIT 1