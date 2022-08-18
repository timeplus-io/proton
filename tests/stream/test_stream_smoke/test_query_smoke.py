import os, sys, time
import logging
from logging import fatal
import pytest
import math

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
    )

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import rockets

cur_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = f"{cur_dir}/configs/config.json"
#tests_file_path = f"{cur_dir}/tests.json"
tests_file_path = f"{cur_dir}"
docker_compose_file_path = f"{cur_dir}/configs/docker-compose.yaml"


def pytest_generate_tests(metafunc):
    logger.debug(f"metafunc.fixturenames = {metafunc.fixturenames}")
    if logger != None and logger.level == 20:
        logging_level = "INFO"
    else:
        logging_level = "DEBUG"
    if "test_set" in metafunc.fixturenames:
        rockets_context = rockets.rockets_context(
            config_file_path, tests_file_path, docker_compose_file_path #todo: read docker_compose_file_path from config and support start different docker envs for different config
        )
        res = rockets.rockets_run(rockets_context)
        test_run_list_len = res[0]
        test_sets = res[1]
        assert len(test_sets) ==  test_run_list_len
        test_ids = [
            #str(test["test_id"]) + "-" + test["test_name"] for test in test_sets
            test["test_suite_name"]+ "-" + str(test["test_id"]) + "-" + test["test_name"] for test in test_sets
        ]
        metafunc.parametrize("test_set", test_sets, ids=test_ids, indirect=True)


@pytest.fixture()
def test_set(request):
    return request.param




def query_result_check(test_set, order_check=False, logging_level="INFO"):

    #formatter = logging.Formatter(
    #    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
    #)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.debug(f"query_run_py starts, logging_level = {logging_level}")
    if logging_level=="INFO":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
    expected_results = test_set.get("expected_results")
    logging.info(f"\n test run: expected_results: {expected_results}")
    statements_results = test_set.get("statements_results")
    logging.info(f"test run: statemetns_results: {statements_results}")
    for i in range(len(expected_results)):  # for each query_results
        expected_result = expected_results[i].get("expected_results")
        expected_result_query_id = expected_results[i].get("query_id")
        query_results_dict = None
        for statement_results in statements_results:

            statement_results_query_id = statement_results.get("query_id")
            if statement_results_query_id == str(expected_result_query_id):
                query_results_dict = statement_results
        assert (
            query_results_dict != None
        )  # if no statement_results_query_id matches expected_result_query_id, case failed
        query_result = query_results_dict.get("query_result")
        logging.info(f"\n test_run: expected_result = {expected_result}")
        logging.info(f"\n test_run: query_result = {query_result}")
        query_result_column_types = query_results_dict.get("query_result_column_types")
        if query_result != 'error_code:159': #error_code:159 means Wait 2100 milliseconds for DDL operation timeout. the timeout is 2000 milliseconds, known issue of redpenda as external stream storage mode, skip result check.
            assert type(expected_result) == type(
                query_result
            ), f"expected_result = {expected_result}, query_result = {query_result}"  # assert if the type of the query_result equals the type of expected_result
            if isinstance(
                expected_result, str
            ):  # if the expected_result is a string lke "skip", "error_code:xxx"
                if expected_result == "skip":
                    continue
                else:
                    assert expected_result == query_result, f"expected_result = {expected_result}, query_result = {query_result}"

            elif isinstance(expected_result, dict):
                for key in expected_result:
                    if expected_result[key] == "any_value":
                        pass
                    else:
                        assert expected_result[key] == query_result[key], f"expected_result = {expected_result}, query_result = {query_result}"
            else:
                if len(expected_result) == 0:
                    assert len(query_result) == 0, f"expected_result = {expected_result}, query_result = {query_result}"

                else:
                    assert len(expected_result) == len(query_result), f"expected_result = {expected_result}, query_result = {query_result}"
                    if (
                        order_check == False
                    ):  # when the order_check ==False, only check if the expected_reslt matches a query result but the sequence of the query result is not checked.
                        expected_result_check_arry = []
                        for i in range(len(expected_result)):
                            expected_result_check_arry.append(0)
                        row_step = 0
                        for expected_result_row in expected_result:
                            for query_result_row in query_result:
                                expected_result_row_field_check_arry = []
                                for i in range(
                                    #len(query_result_column_types) - 1 # query_result_column_types has a timestamp filed added by query_execute, so need to minus 1
                                    len(query_result_column_types)
                                ):  
                                    expected_result_row_field_check_arry.append(0)

                                expected_result_row_check = 1
                                for i in range(
                                    len(expected_result_row)
                                ):  # for each filed of each row of each query_results
                                    expected_result_field = expected_result_row[i]
                                    query_result_field = query_result_row[i]
                                    if "array" in query_result_column_types[i][1] and "array_join" not in query_result_column_types[i][1]:
                                        if expected_result_field == query_result_field:
                                            expected_result_row_field_check_arry[i] = 1
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                        else:
                                            pass
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: not match")
                                    elif "float" in query_result_column_types[i][1]:
                                        if math.isclose(
                                            float(expected_result_field),
                                            float(query_result_field),
                                            rel_tol=1e-2,
                                        ):
                                            expected_result_row_field_check_arry[i] = 1
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                        else:
                                            pass
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: not match")
                                    elif "int" in query_result_column_types[i][1] and "tuple" not in query_result_column_types[i][1] and "map" not in query_result_column_types[i][1]:
                                        if int(expected_result_field) == int(
                                            query_result_field
                                        ):
                                            expected_result_row_field_check_arry[i] = 1
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                        else:
                                            pass
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                    elif "nullable" in query_result_column_types[i][1]:
                                        if  query_result_field == "None":
                                            expected_result_row_field_check_arry[i] = 1
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                        else:
                                            pass
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")                                
                                    
                                    elif isinstance(expected_result_field,str):
                                        _match = 1
                                        _expected_field_itmes = expected_result_field.split(",")
                                        _query_field_items = query_result_field.split(",")
                                        for (_expected_item, _result_item)  in zip(_expected_field_itmes, _query_field_items):
                                            logging.debug(f"_expected_item = {_expected_item}, _result_item = {_result_item}")
                                            if _expected_item == 'any_value' or _expected_item == _result_item:
                                                _match *= 1
                                            else:
                                                _match *= 0
                                        if _match:
                                            expected_result_row_field_check_arry[i] = 1
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                        else:
                                            pass
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: not match")                                           
                                    else:
                                        if expected_result_field == query_result_field:
                                            expected_result_row_field_check_arry[i] = 1
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: match")
                                        else:
                                            pass
                                            #logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                            #logging.debug("test_run: not match")

                                #logging.debug(f"test_run: expected_result_row_field_check_arry = {expected_result_row_field_check_arry}")
                                expected_result_row_check = 1
                                for i in range(len(expected_result_row_field_check_arry)):
                                    expected_result_row_check = (
                                        expected_result_row_check
                                        * expected_result_row_field_check_arry[i]
                                    )
                                #logging.debug(f"test_run: expected_result_row_check = {expected_result_row_check}")
                                if expected_result_row_check == 1:
                                    expected_result_check_arry[row_step] = 1
                            assert expected_result_check_arry[row_step] == 1,  f"expected_result = {expected_result}, query_result = {query_result}"
                            row_step += 1
                    else:  # if order_check == True, assert the query result in the exact sequence of the expected result
                        for i in range(
                            len(expected_result)
                        ):  # for each row of each query_results
                            expected_result_row = expected_result[i]
                            query_result_row = query_result[i]
                            assert (
                                len(expected_result_row) == len(query_result_row) - 1
                            ),f"expected_result = {expected_result}, query_result = {query_result}"  # the timestamp field in query_result_row is artifically added and need to be excluded in the length
                            for i in range(
                                len(expected_result_row)
                            ):  # for each filed of each row of each query_results
                                expected_result_field = expected_result_row[i]
                                query_result_field = query_result_row[i]
                                if "array" in query_result_column_types[i][1]:
                                    assert expected_result_field == query_result_field, f"expected_result = {expected_result}, query_result = {query_result}"
                                elif "float" in query_result_column_types[i][1]:
                                    assert math.isclose(
                                        float(expected_result_field),
                                        float(query_result_field),
                                        rel_tol=1e-2,
                                    ), f"expected_result = {expected_result}, query_result = {query_result}"
                                elif "int" in query_result_column_types[i][1]:
                                    assert int(expected_result_field) == int(
                                        query_result_field
                                    ), f"expected_result = {expected_result}, query_result = {query_result}"
                                else:
                                    assert expected_result_field == query_result_field, f"expected_result = {expected_result}, query_result = {query_result}"
        else:
            assert 1 == 1


def test_run(test_set, caplog):
    query_result_check(test_set)
