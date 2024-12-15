from chispa.dataframe_comparer import *
from ..jobs.actors_history_scd_backfill import do_actors_history_scd_backfill_transformation
from collections import namedtuple
Actors = namedtuple("Actors", "actorid actor current_year quality_class is_active")
ActorsScd = namedtuple("ActorsScd", "actorid actor quality_class is_active start_year end_year")


def test_scd_generation(spark):
    source_data = [
        Actors("nm0000019", "Federico Fellini", 1972, 'good', True),
        Actors("nm0000019", "Federico Fellini", 1973, 'good', False),
        Actors("nm0000019", "Federico Fellini", 1986, 'good', False),
        Actors("nm0000019", "Federico Fellini", 1987, 'average', True),
        Actors("nm0000019", "Federico Fellini", 1988, 'average', False),
        Actors("nm0000019", "Federico Fellini", 2021, 'average', False)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_history_scd_backfill_transformation(spark, source_df)
    expected_data = [
        ActorsScd("nm0000019", "Federico Fellini", 'good', True, 1972, 1972),
        ActorsScd("nm0000019", "Federico Fellini", 'good', False, 1973, 1986),
        ActorsScd("nm0000019", "Federico Fellini", 'average', True, 1987, 1987),
        ActorsScd("nm0000019", "Federico Fellini", 'average', False, 1988, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)