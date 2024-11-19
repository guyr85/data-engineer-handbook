CREATE TYPE season_stats AS (
    season Integer,
    gp Integer,
    pts REAL,
    reb REAL,
    ast REAL,
    weight INTEGER
    );

CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');


 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scorer_class scoring_class,
     is_active BOOLEAN,
     current_season INTEGER,
	 years_since_last_active INTEGER,
     PRIMARY KEY (player_name, current_season)
 );
