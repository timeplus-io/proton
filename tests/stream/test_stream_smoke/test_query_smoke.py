import os, sys
from logging import fatal
import pytest
import math


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import helpers.rockets


def pytest_generate_tests(metafunc):
    if "test_set" in metafunc.fixturenames:
        rockets_context = helpers.rockets.rockets_context(
            "./configs/config.json", "./tests.json"
        )
        test_sets = helpers.rockets.rockets_run(rockets_context)
        test_ids = [test["test_name"] for test in test_sets]
        metafunc.parametrize("test_set", test_sets, ids=test_ids, indirect=True)


@pytest.fixture()
def test_set(request):
    return request.param


def test_run(test_set, caplog):
    expected_results = test_set.get("expected_results")
    statements_results = test_set.get("statements_results")

    for i in range(len(expected_results)):  # for each query_results
        expected_result = expected_results[i]
        print(f"test run: expected_result in expected_results: {expected_result}")
        query_results_dict = statements_results[i]
        query_result = query_results_dict.get("query_result")
        query_result_column_types = query_results_dict.get("query_result_column_types")
        assert type(expected_result) == type(
            query_result
        )  # assert if the type of the query_result equals the type of expected_result
        if isinstance(
            expected_result, str
        ):  # if the expected_result is a string lke "skip", "error_code:xxx"
            if expected_result == "skip":
                continue
            else:
                assert expected_result == query_result
        else:
            for i in range(len(expected_result)):  # for each row of each query_results
                expected_result_row = expected_result[i]
                # print(f"i in expected_result = {i}, expected_result_row={expected_result_row}")
                query_result_row = query_result[i]
                # print(f"i in expected_result = {i}, query_result_row={query_result_row}")
                assert (
                    len(expected_result_row) == len(query_result_row) - 1
                )  # the timestamp field in query_result_row is artifically added and need to be excluded in the length
                for i in range(
                    len(expected_result_row)
                ):  # for each filed of each row of each query_results
                    # print("test_run: column_type:", query_result_column_types[i][1])
                    expected_result_field = expected_result_row[i]
                    query_result_field = query_result_row[i]
                    if "Float" in query_result_column_types[i][1]:
                        assert math.isclose(
                            float(expected_result_field),
                            float(query_result_field),
                            rel_tol=1e-2,
                        )
                    elif "Int" in query_result_column_types[i][1]:
                        assert int(expected_result_field) == int(query_result_field)
                    else:
                        assert expected_result_field == query_result_field
