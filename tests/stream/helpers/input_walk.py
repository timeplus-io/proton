import pytest
from pytest_cases import parametrize_with_cases

import os, sys, json, getopt
import time
import datetime
import re
import random
from pytest_cases.fixture_parametrize_plus import parametrize
import requests

# from dataclasses import dataclass
import multiprocessing as mp
from clickhouse_driver import Client
from clickhouse_driver import errors
from requests.api import request


def input_walk(test_id_run, config_file, tests_file):
    input_results = []
    with open(config_file) as f:
        config = json.load(f)
    #    proton_server = config.get('proton_server')
    #    proton_server_native_port = config.get('proton_server_native_port')
    with open(tests_file) as f:
        test_suit = json.load(f)
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    tests = test_suit.get("tests")
    rest_setting = config.get("rest_setting")
    table_schema = config.get("table_schema")
    client = Client(host=proton_server, port=proton_server_native_port)
    inputs = tests[test_id_run].get("inputs")
    columns = table_schema.get("columns")
    table_name = table_schema.get("name")
    table_columns = ""
    for element in columns:
        table_columns = table_columns + element.get("name") + ","
    table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
    # print("input_walk_through: table_columns_str = ", table_columns_str)

    if inputs:
        # print("input_walk_through: inputs:", inputs)
        for batch in inputs:
            # print("input_walk_through: batch:", batch)
            batch_str = " "
            for row in batch:
                # print("input_walk_through: row:", row)
                row_str = " "
                for field in row:
                    # print("input_walk_through: field:", field)
                    if isinstance(field, str):
                        field.replace('"', '//"')  # proton does
                    row_str = (
                        row_str + "'" + str(field) + "'" + ","
                    )  # python client does not support "", so put ' here
                row_str = "(" + row_str[: len(row_str) - 1] + ")"
                # print("row_str:", row_str)
                batch_str = batch_str + row_str + ","
                # print("batch_str:", batch_str)
            batch_str = batch_str[: len(batch_str) - 1]
            # print("batch_str", batch_str)
            # insert_sql = "insert into {} {} values {}".format(table_name, table_columns_str, batch_str[:len(batch_str)-1])
            input_sql = (
                f"insert into {table_name} {table_columns_str} values {batch_str}"
            )
            # print("input_walk_through: input_sql= ", input_sql)
            input_result = client.execute(input_sql)
            print("input_walk: input_sql= {}, done.".format(input_sql))
            print("--------------------------------------------------")
            # print("input_walk_through: input_result= ", input_result)
            input_results.append(input_result)
        else:
            print("no inputs in this test_id_run")
    return input_results


if __name__ == "__main__":
    test_suite_path = None
    test_id_run = None
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
