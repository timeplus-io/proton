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
        print(f"pytest_generate_tests: config_file_path = {config_file_path}")
        rockets_context = rockets.rockets_context(
            config_file_path, tests_file_path, docker_compose_file_path #todo: read docker_compose_file_path from config and support start different docker envs for different config
        )
        res = rockets.rockets_run(rockets_context)
        test_run_list_len = res[0]
        test_run_list_total = res[1]
        test_sets = res[2]
        assert len(test_sets) ==  test_run_list_len
        test_ids = [
            #str(test["test_id"]) + "-" + test["test_name"] for test in test_sets
            test["test_suite_name"]+ "-" + str(test["test_id"]) + "-" + test["test_name"] for test in test_sets
        ]
        metafunc.parametrize("test_set", test_sets, ids=test_ids, indirect=True)


@pytest.fixture()
def test_set(request):
    return request.param


def case_result_check(test_set, logging_level="INFO"):
    #formatter = logging.Formatter(
    #    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
    #)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    #logger.addHandler(console_handler)
    if logging_level=="INFO":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    expected_results = test_set.get("expected_results")
    statements_results_designed = test_set.get("statements_results_designed")
    logger.info(f"expected_results = {expected_results}, statements_results_designed = {statements_results_designed}")

    assert test_set["test_result"] == True, f"expected_result = {expected_results} \n, statements_results_designed = {statements_results_designed}"    



def test_run(test_set, caplog):
    case_result_check(test_set)
