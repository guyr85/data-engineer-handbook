from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark sessions
spark = SparkSession.builder.appName("homework_spark_fundamentals").getOrCreate()

# Read tables to dataframes for homework purpose
df_match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
df_matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
df_medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
df_medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
df_maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

# Was asked to disabled automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Explicitly broadcast JOINs dims `medals` and `maps` with fact tables `medals_matches_players` and `matches`
df_medals_matches_players_with_medals_details = df_medals_matches_players.join(F.broadcast(df_medals), "medal_id")
df_matches_with_maps_details = df_matches.join(F.broadcast(df_maps), "mapid")

df_medals_matches_players_with_medals_details.printSchema()
df_matches_with_maps_details.printSchema()

# Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
# I'll take the df_medals_matches_players_with_medals_details and df_matches_with_maps_details as enriched tables with dims attributes

# First we will create ddl for each of the buckets table

# DDL For match_details_bucketed
match_details_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(match_details_bucketed_ddl)

# DDL For matches_with_maps_details_bucketed
matches_with_maps_details_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_with_maps_details_bucketed (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP,
    map_name STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(matches_with_maps_details_bucketed_ddl)

# DDL For medals_matches_players_with_medals_details_bucketed
medals_matches_players_with_medals_details_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_with_medals_details_bucketed (
    match_id STRING,
    medal_id STRING,
    count_medals INTEGER,
    medal_classification STRING,
    medal_name STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(medals_matches_players_with_medals_details_ddl)


# Write bucketed tables to disk
df_match_details.select("match_id",
                        "player_gamertag",
                        F.col("player_total_kills").cast("int"),
                        F.col("player_total_deaths").cast("int")
                       ) \
    .write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")
df_matches_with_maps_details.select("match_id",
                                    "mapid",
                                    "is_team_game",
                                    "playlist_id" ,
                                    F.col("completion_date").cast("timestamp"),
                                    F.col("name").alias("map_name")) \
    .write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.matches_with_maps_details_bucketed")

df_medals_matches_players_with_medals_details.select("match_id",
                                                     "medal_id",
                                                     F.col("count").cast("int").alias("count_medals"),
                                                     F.col("classification").alias("medal_classification"),
                                                     F.col("name").alias("medal_name")) \
    .write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.medals_matches_players_with_medals_details_bucketed")

# Read bucketed tables
match_details_b = spark.table("bootcamp.match_details_bucketed")
matches_b = spark.table("bootcamp.matches_with_maps_details_bucketed")
medal_matches_players_b = spark.table("bootcamp.medals_matches_players_with_medals_details_bucketed")

# Perform a join (bucket join happens automatically)
agg_result = match_details_b \
    .join(matches_b, "match_id") \
    .join(medal_matches_players_b, "match_id")

## Figuring out questions like the following from the data
## Which player averages the most kills per game?

# Select relevant columns for the analysis
# Dedup rows since every match_id can have more than one raw due to different medals in a game
# Calculate the avg kills for a player
# Show the player with the most kills per game
agg_result \
    .select("match_id", "player_gamertag", "player_total_kills") \
    .distinct() \
    .groupBy("player_gamertag") \
    .agg(F.avg("player_total_kills").alias("avg_kills_perg_game"))\
    .orderBy(F.col("avg_kills_perg_game").desc()) \
    .show(1)

## Which playlist gets played the most?

# Select relevant columns for the analysis
# Dedup rows since every match_id can have more than one raw due to different medals in a game
# Calculate the number of matches for playlist_id
# Show the playlist_id with the most played games
agg_result \
    .select("match_id", "playlist_id") \
    .distinct() \
    .groupBy("playlist_id") \
    .agg(F.count("match_id").alias("cnt_matches"))\
    .orderBy(F.col("cnt_matches").desc()) \
    .show(1)

## Which map gets played the most?

# Select relevant columns for the analysis
# Dedup rows since every match_id can have more than one raw due to different medals in a game
# Calculate the number of matches for map
# Show the map with the most played games
agg_result \
    .select("match_id", "mapid", "map_name") \
    .distinct() \
    .groupBy("mapid", "map_name") \
    .agg(F.count("match_id").alias("cnt_matches"))\
    .orderBy(F.col("cnt_matches").desc()) \
    .show(1)

## Which map do players get the most Killing Spree medals on?

# Select relevant columns for the analysis
# Dedup rows since every match_id can have more than one raw due to different medals in a game
# Calculate the number of matches for map
# Show the map with the most played games
agg_result \
    .select("match_id", "mapid", "map_name", "medal_name", "count_medals") \
    .filter(F.col("medal_name") == F.lit("Killing Spree")) \
    .distinct() \
    .groupBy("mapid", "map_name","medal_name") \
    .agg(F.sum("count_medals").alias("sum_medals"))\
    .orderBy(F.col("sum_medals").desc()) \
    .show(1)

## With the aggregated data set try different `.sortWithinPartitions`
# to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
# I took the dataframe after the join buckets between all tables

# DDL For aggregated data set sort match id
aggregated_data_set_sort_match_id_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.aggregated_data_set_sort_match_id (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    mapid STRING,
    is_team_game STRING,
    playlist_id STRING,
    completion_date TIMESTAMP,
    map_name STRING,
    medal_id STRING,
    count_medals INTEGER,
    medal_classification STRING,
    medal_name STRING
)
USING iceberg;
"""
spark.sql(aggregated_data_set_sort_match_id_ddl)

# DDL For aggregated data set sort mapid
aggregated_data_set_sort_mapid_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.aggregated_data_set_sort_mapid (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    mapid STRING,
    is_team_game STRING,
    playlist_id STRING,
    completion_date TIMESTAMP,
    map_name STRING,
    medal_id STRING,
    count_medals INTEGER,
    medal_classification STRING,
    medal_name STRING
)
USING iceberg;
"""
spark.sql(aggregated_data_set_sort_mapid_ddl)

# DDL For aggregated data set sort playlist
aggregated_data_set_sort_playlists_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.aggregated_data_set_sort_playlists (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    mapid STRING,
    is_team_game STRING,
    playlist_id STRING,
    completion_date TIMESTAMP,
    map_name STRING,
    medal_id STRING,
    count_medals INTEGER,
    medal_classification STRING,
    medal_name STRING
)
USING iceberg;
"""
spark.sql(aggregated_data_set_sort_playlists_ddl)


# Different combination of dataframe with sorted data to check who will have the smallest size
aggregated_data_set_sort_match_id = agg_result.sortWithinPartitions(F.col("match_id"))
aggregated_data_set_sort_mapid = agg_result.sortWithinPartitions(F.col("mapid"))
aggregated_data_set_sort_playlists = agg_result.sortWithinPartitions(F.col("playlist_id"))

aggregated_data_set_sort_match_id.write.mode("overwrite").saveAsTable("bootcamp.aggregated_data_set_sort_match_id")
aggregated_data_set_sort_mapid.write.mode("overwrite").saveAsTable("bootcamp.aggregated_data_set_sort_mapid")
aggregated_data_set_sort_playlists.write.mode("overwrite").saveAsTable("bootcamp.aggregated_data_set_sort_playlists")

# I run the following queries to find the size of the files and num of files
"""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_match_id' 
FROM bootcamp.aggregated_data_set_sort_match_id.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_mapid' 
FROM bootcamp.aggregated_data_set_sort_mapid.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_playlist_id' 
FROM bootcamp.aggregated_data_set_sort_playlists.files
"""
# I found out that the smallest data size is in the following order: sorted_match_id, sorted_playlist_id, sorted_mapid
