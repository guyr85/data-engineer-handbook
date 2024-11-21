CREATE TYPE films AS (
	filmid TEXT,
    film TEXT,
    votes Integer,
    rating REAL,
	year Integer
    );

CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');


 CREATE TABLE actors (
     actorid TEXT,
     actor TEXT,
     films films[],
     quality_class quality_class,
     is_active BOOLEAN,
     current_year INTEGER,
     PRIMARY KEY (actorid, current_year)
 );