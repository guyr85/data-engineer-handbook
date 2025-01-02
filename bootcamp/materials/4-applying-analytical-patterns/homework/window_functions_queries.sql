-- What is the most games a team has won in a 90 game stretch?

-- Explanation:

-- game_details_clean_duplicates:
-- Removes duplicate rows from the game_details table by selecting only unique rows based on the
-- team_abbreviation, game_id, game_date_est, team_id, team_id_home, team_id_away, and home_team_wins columns.

-- ROW_NUMBER():
-- Assigns an incremental game_number to each game for every team_id, ordered by game_date.

-- Sliding Window Frame:
-- Uses SUM(win) with a window frame of ROWS BETWEEN 89 PRECEDING AND CURRENT ROW to calculate the number of wins in the last 90 games (including the current game).

-- MAX(win_stretch):
-- Finds the maximum number of wins within any 90-game stretch for each team.

-- Limit Results:
-- Orders teams by their maximum win streaks and limits the result to the highest streak.

with game_details_clean_duplicates AS (
SELECT DISTINCT
        gd.team_abbreviation,
        gd.game_id,
		g.game_date_est,
        gd.team_id,
		g.team_id_home,
		g.team_id_away,
		g.home_team_wins
    FROM game_details gd
    JOIN games g
		ON gd.game_id = g.game_id
),
game_results AS (
    SELECT DISTINCT
        team_abbreviation,
        game_id,
        ROW_NUMBER() OVER (PARTITION BY team_abbreviation ORDER BY game_date_est) AS game_number,
        CASE
            WHEN team_id = team_id_home AND home_team_wins = 1 THEN 1
			WHEN team_id = team_id_away AND home_team_wins = 0 THEN 1
			ELSE 0
        END AS win
    FROM game_details_clean_duplicates

)

SELECT
    team_abbreviation,
    MAX(win_stretch) AS max_wins_in_90_games
FROM (
    SELECT
        team_abbreviation,
        SUM(win) OVER (
            PARTITION BY team_abbreviation
            ORDER BY game_number
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS win_stretch
    FROM game_results
) team_stretch
GROUP BY team_abbreviation
ORDER BY max_wins_in_90_games DESC
LIMIT 1;


-- How many games in a row did LeBron James score over 10 points a game?

-- Explanation:
-- Streak Identification:
-- ROW_NUMBER() - SUM(...): This trick identifies groups of consecutive games where the condition (pts > 10) is true
-- by creating a unique streak_id for each streak.

-- Streak Length Calculation:
-- Each streak (streak_id) is grouped, and the number of games in that streak is calculated using COUNT(*).

-- Longest Streak:
-- The longest streak is identified using MAX(streak_length).

WITH player_scores AS (
    SELECT
        gd.player_id,
        g.game_date_est,
        CASE
            WHEN gd.pts > 10 THEN 1
            ELSE 0
        END AS scored_above_10,
        ROW_NUMBER() OVER (PARTITION BY gd.player_id ORDER BY g.game_date_est)
            - SUM(CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END)
              OVER (PARTITION BY gd.player_id ORDER BY g.game_date_est) AS streak_id
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE lower(gd.player_name) = 'lebron james'
)
SELECT
    player_id,
    MAX(streak_length) AS longest_streak
FROM (
    SELECT
        player_id,
        streak_id,
        COUNT(*) AS streak_length
    FROM player_scores
    WHERE scored_above_10 = 1
    GROUP BY player_id, streak_id
) streaks
GROUP BY player_id;