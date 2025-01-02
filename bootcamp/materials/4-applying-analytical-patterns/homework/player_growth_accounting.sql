 CREATE TABLE player_growth_accounting (
     player_name TEXT,
     first_active_year INTEGER,
     last_active_year INTEGER,
     yearly_active_state TEXT,
     years_active INTEGER[],
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );

insert into player_growth_accounting
-- Last year players data
WITH yesterday AS (
    SELECT * FROM player_growth_accounting
    WHERE current_season = 2001
),
    -- Current year players data
     today AS (
         SELECT
            player_name,
            season as current_season,
            COUNT(1)
         FROM player_seasons
         WHERE season = 2002
         AND player_name IS NOT NULL
         GROUP BY player_name, season
     )
        -- Doing an incremental load of the player_growth_accounting table by comparing the data from the last year with the data from the current year
         SELECT COALESCE(t.player_name, y.player_name) as player_name,
                COALESCE(y.first_active_year, t.current_season) AS first_active_year,
                COALESCE(t.current_season, y.last_active_year) AS last_active_year,
                CASE
                    WHEN y.player_name IS NULL THEN 'New'
					WHEN t.current_season IS NULL AND y.last_active_year = y.current_season THEN 'Retired'
					WHEN y.last_active_year = t.current_season - 1 THEN 'Continued Playing'
					WHEN y.last_active_year < t.current_season - 1 THEN 'Returned from Retirement'
                    ELSE 'Stayed Retired'
                    END as yearly_active_state,
                COALESCE(y.years_active,
                         ARRAY []::INTEGER[])
                    || CASE
                           WHEN
                               t.player_name IS NOT NULL
                               THEN ARRAY [t.current_season]
                           ELSE ARRAY []::INTEGER[]
                    END AS years_active,
                COALESCE(t.current_season, y.current_season + 1) as current_season
         FROM today t
         FULL OUTER JOIN yesterday y
         	ON t.player_name = y.player_name;