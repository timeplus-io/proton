import logging, logging.config

import pytest
from pytest_cases import parametrize_with_cases

import os, sys, json, getopt
import time
import datetime
import re
import random
from pytest_cases.fixture_parametrize_plus import parametrize
import requests

import multiprocessing as mp
from proton_driver import Client
from proton_driver import errors
from requests.api import request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.rockets import env_setup
from helpers.rockets import reset_tables_of_test_inputs
from helpers.rockets import scan_tests_file_path
from helpers.rockets import input_walk_through_rest, input_batch_rest, find_schema


logger = logging.getLogger(__name__)

'''
def input_batch_rest(input_url, input_batch, table_schema):
    # todo: complete the input by rest
    logger.debug(f"input_batch_rest: input_batch = {input_batch}")
    input_batch_record = {}
    table_name = table_schema.get("name")
    input_rest_columns = []
    input_rest_body_data = []
    input_rest_body = {"columns": input_rest_columns, "data": input_rest_body_data}
    for element in table_schema.get("columns"):
        input_rest_columns.append(element.get("name"))
    logger.debug(f"input_batch_rest: input_rest_body = {input_rest_body}")
    input_batch_data = input_batch.get("data")
    for row in input_batch_data:
        logger.debug(f"input_batch_rest: row_data = {row}")
        input_rest_body_data.append(
            row
        )  # get data from inputs batch dict as rest ingest body.
    input_rest_body = json.dumps(input_rest_body)
    input_url = f"{input_url}/{table_name}"
    res = requests.post(input_url, data=input_rest_body)

    assert res.status_code == 200
    input_batch_record["input_batch"] = input_rest_body_data
    input_batch_record["timestamp"] = str(datetime.datetime.now())

    return input_batch_record


def find_schema(table_name, table_schemas):
    for table_schema in table_schemas:
        if table_name == table_schema.get("name"):
            return table_schema
    return None


def input_walk_through_rest(
    rest_setting,
    inputs,
    table_schemas,
    wait_before_inputs=1,
    sleep_after_inputs=1.5,  # stable set wait_before_inputs=1, sleep_after_inputs=1.5
):
    wait_before_inputs = wait_before_inputs  # the seconds sleep before inputs starts to ensure the query is run on proton.
    sleep_after_inputs = sleep_after_inputs  # the seconds sleep after evary inputs of a case to ensure the stream query result was emmited by proton and received by the query execute
    time.sleep(wait_before_inputs)
    input_url = rest_setting.get("ingest_url")
    inputs_record = []
    print(f"input_walk_through_rest: inputs = {inputs}")

    for batch in inputs:
        table_name = batch.get("table_name")
        wait = batch.get("wait")
        if wait != None:
            time.sleep(wait)
        table_schema = find_schema(table_name, table_schemas)
        if table_schema != None:
            batch_sleep_before_input = batch.get("sleep")
            if batch_sleep_before_input != None:
                time.sleep(int(batch_sleep_before_input))
            logger.debug(f"input_walk_through_rest: table_schema = {table_schema}")
            input_batch_record = input_batch_rest(input_url, batch, table_schema)
            inputs_record.append(input_batch_record)
        else:
            logger.debug(
                f"input_walk_through_rest: table_schema of table name {table_schema} not founded in table schemas {table_schemas}"
            )
        # time.sleep(0.5)
    time.sleep(sleep_after_inputs)
    return inputs_record
'''

def input_walk(test_id_run, config_file, test_suite, setup = False):
    input_results = []
    with open(config_file) as f:
        config = json.load(f)

    #with open(tests_file) as f:
    #    test_suite = json.load(f)

    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    test_suite_config = test_suite.get("test_suite_config")
    table_schemas = test_suite_config.get("table_schemas")
    tests = test_suite.get("tests")
    tests_2_run = test_suite_config.get("tests_2_run")
    test_run_list = []
    #proton_ci_mode = os.getenv("PROTON_CI_MODE", "Github")
    #logging.info(f"rockets_run: proton_ci_mode = {proton_ci_mode}")
    client = Client(host=proton_server, port=proton_server_native_port)
    if setup:
        env_setup(
            client, rest_setting, test_suite_config
        )

    for test in tests:
        test_id = str(test.get("id"))
        if test_id == str(test_id_run):
            logger.debug(f"input_walk: test_id = {test_id} ...... test = {test}...... test_id_run = {test_id_run}")
            steps = test.get("steps")
            logger.debug(f"input_walk: steps = {steps}")
            for step in steps:
                print(f"input_walk: step = {step}")
                if "inputs" in step:
                    inputs = step.get("inputs")
                    print(f"input_walk: test_id = {test_id} inputs = {inputs}")
                    inputs_record = input_walk_through_rest(
                        rest_setting, inputs, table_schemas
                    )
                    input_results.append(inputs_record)

    return input_results


if __name__ == "__main__":
    cur_file_path = os.path.dirname(os.path.abspath(__file__))
    cur_file_path_parent = os.path.dirname(cur_file_path)
    test_suite_path = None
    test_suite_name_list = None

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"  
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

    logger.info("input_walk starts......")

    test_suite_path = "." #default ./
    setup = False
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'i:d:', ["setup", "test_suites="])
        for name, value in opts:
            if name in ["-i"]:
                test_id_run = int(value)
            if name in ["-d"]:
                test_suite_path = value
            if name in ["--setup"]:
                setup = True
            if name in ("--test_suites"):
                test_suite_name_list = value.split(',')
                os.environ["PROTON_TEST_SUITES"] = value            
    except:
        print("Error")

    config_file = f"{test_suite_path}/configs/config.json"
    #tests_file = f"{test_suite_path}/tests.json"
    res_scan_tests_file_path = scan_tests_file_path(test_suite_path)
    test_suite_names_selected = res_scan_tests_file_path.get("test_suite_names_selected")
    test_suites_selected = res_scan_tests_file_path.get("test_suites_selected")
    if test_suites_selected != None and len(test_suites_selected) != 0:
        test_suite = test_suites_selected[0]
        logger.debug(f"test_suite = {test_suite}")

    input_walk(test_id_run, config_file, test_suite, setup)
