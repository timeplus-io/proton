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
from clickhouse_driver import Client
from clickhouse_driver import errors
from requests.api import request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import rockets


def input_batch_rest(input_url, input_batch, table_schema):
    # todo: complete the input by rest
    logging.debug(f"input_batch_rest: input_batch = {input_batch}")
    input_batch_record = {}
    table_name = table_schema.get("name")
    input_rest_columns = []
    input_rest_body_data = []
    input_rest_body = {"columns": input_rest_columns, "data": input_rest_body_data}
    for element in table_schema.get("columns"):
        input_rest_columns.append(element.get("name"))
    logging.debug(f"input_batch_rest: input_rest_body = {input_rest_body}")
    input_batch_data = input_batch.get("data")
    for row in input_batch_data:
        logging.debug(f"input_batch_rest: row_data = {row}")
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
        table_schema = find_schema(table_name, table_schemas)
        if table_schema != None:
            batch_sleep_before_input = batch.get("sleep")
            if batch_sleep_before_input != None:
                time.sleep(int(batch_sleep_before_input))
            logging.debug(f"input_walk_through_rest: table_schema = {table_schema}")
            input_batch_record = input_batch_rest(input_url, batch, table_schema)
            inputs_record.append(input_batch_record)
        else:
            logging.debug(
                f"input_walk_through_rest: table_schema of table name {table_schema} not founded in table schemas {table_schemas}"
            )
        # time.sleep(0.5)
    time.sleep(sleep_after_inputs)
    return inputs_record


def input_walk(test_id_run, config_file, tests_file):
    input_results = []
    with open(config_file) as f:
        config = json.load(f)

    with open(tests_file) as f:
        test_suite = json.load(f)

    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    test_suite_config = test_suite.get("test_suite_config")
    table_schemas = test_suite_config.get("table_schemas")
    tests = test_suite.get("tests")
    tests_2_run = test_suite_config.get("tests_2_run")
    test_run_list = []
    proton_ci_mode = os.getenv("PROTON_CI_MODE", "Github")
    logging.info(f"rockets_run: proton_ci_mode = {proton_ci_mode}")
    client = Client(host=proton_server, port=proton_server_native_port)

    for test in tests:
        test_id = str(test.get("id"))
        if test_id == str(test_id_run):
            logging.debug(f"input_walk: test_id = {test_id} ...... test = {test}...... test_id_run = {test_id_run}")
            steps = test.get("steps")
            logging.debug(f"input_walk: steps = {steps}")
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
    logging_config_file = f"{cur_file_path}/logging.conf"
    if os.path.exists(logging_config_file):
        logging.basicConfig(
            format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
        )  # todo: add log stuff
        logging.config.fileConfig(logging_config_file)  # need logging.conf
        logger = logging.getLogger("rockets")
    else:
        print("no logging.conf exists under ../helper, no logging.")

    logging.info("rockets_main starts......")

    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, "i:")
        for opt, arg in opts:
            if opt in ["-i"]:
                test_id_run = int(arg)
    except:
        print("Error")

    test_suite_path = "."
    config_file = f"{test_suite_path}/configs/config.json"
    tests_file = f"{test_suite_path}/tests.json"

    input_walk(test_id_run, config_file, tests_file)
