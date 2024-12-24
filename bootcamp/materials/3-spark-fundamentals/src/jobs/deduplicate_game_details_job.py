from pyspark.sql import SparkSession


def do_deduplicate_game_details_transformation(spark, dataframe):
    query = f"""
    with game_details_dedup_rows AS (
	SELECT *,
		   row_number() over (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS rn
	FROM game_details
    )
    
    SELECT game_id, team_id, player_id
    FROM game_details_dedup_rows
    WHERE rn = 1
    """
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("test_deduplicate_game_details_job") \
      .getOrCreate()
    output_df = do_deduplicate_game_details_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("deduplicate_game_details")