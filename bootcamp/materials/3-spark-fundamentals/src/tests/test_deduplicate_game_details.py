from chispa.dataframe_comparer import *
from ..jobs.deduplicate_game_details_job import do_deduplicate_game_details_transformation
from collections import namedtuple
GameDetails = namedtuple("GameDetails", "game_id team_id player_id")
DeduplicateGameDetails = namedtuple("DeduplicateGameDetails", "game_id team_id player_id")


def test_scd_generation(spark):
    source_data = [
        GameDetails(1, 10, 100),
        GameDetails(1, 10, 100),
        GameDetails(2, 10, 100),
        GameDetails(1, 10, 100)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_deduplicate_game_details_transformation(spark, source_df)
    expected_data = [
        DeduplicateGameDetails(1, 10, 100),
        DeduplicateGameDetails(2, 10, 100)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
