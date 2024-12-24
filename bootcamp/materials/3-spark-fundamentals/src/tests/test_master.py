from ..tests.test_deduplicate_game_details import test_deduplicate_game_details
from ..tests.test_player_scd import test_scd_generation
import traceback


def test_master(spark):
    # print("Running Master Test")
    # try:
    #     test_scd_generation(spark)
    #     test_deduplicate_game_details(spark)
    #     print("Finished Master Test")
    #
    # except Exception as e:
    #     print(f"Failed Master Test")
    #     # Extract the traceback and get the function name
    #     tb = traceback.extract_tb(e.__traceback__)
    #     if tb:
    #         # Get the function name in the traceback for the failing test
    #         failing_test = tb[1].name
    #         print(f"Exception occurred in test: {failing_test}")
    #
    #     raise e
    print("\n####################")
    print("Running Master Test")
    print("####################")
    failures = []  # List to collect failure details

    # List of tests to run
    tests = [
        ("test_scd_generation", test_scd_generation),
        ("test_deduplicate_game_details", test_deduplicate_game_details),
    ]

    for test_name, test_func in tests:
        try:
            print(f"Running {test_name}")
            test_func(spark)
            print(f"{test_name} passed")
        except Exception as e:
            print(f"{test_name} failed")
            # Collect failure details
            tb = traceback.extract_tb(e.__traceback__)
            failing_function = tb[1].name if tb else test_name
            failing_test = tb[-1].name if tb else test_name
            failures.append({
                "function_name": failing_function,
                "test_name": failing_test,
                "error": str(e),
                "traceback": "".join(traceback.format_exception(type(e), e, e.__traceback__))
            })

    # Report all failures
    if failures:
        print("\n=== Failure Report ===")
        for failure in failures:
            print(f"Function: {failure['function_name']}")
            print(f"Test: {failure['test_name']}")
            print(f"Error: {failure['error']}")
            print(f"Traceback:\n{failure['traceback']}")
        raise Exception(f"{len(failures)} tests failed")

    print("####################")
    print("Finished Master Test")
    print("####################")
