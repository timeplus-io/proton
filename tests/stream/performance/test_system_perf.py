import os, sys
import logging
from logging import fatal
import pytest
import math


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import bucks

if __name__ == "__main__":
    bucks.run()




'''
    logging_setup()
    logging.info(f"{__file__}: {__name__} starts......")
    argv = sys.argv[1:]  # get -f to specify the test_sutie path
    try:
        opts, args = getopt.getopt(argv, "f:")
    except:
        print("Error")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-f"]:
            csv_file_path = arg

    print(f"main: csv_file_path = {csv_file_path}")

    proton_server = "localhost"
    proton_server_native_port = "9000"

    test_context = rockets.systest_context("./configs/config.json", "./tests.json")
    # logging.debug(f"{__file__}: {__name__}: rockets_systest_context_info = {rockets_systest_context_info}")
    config = test_context.get("config")
    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    test_suite = test_context.get("test_suite")
    test_suite_config = test_suite.get("test_suite_config")

    table_schema = test_suite_config.get("table_schemas")[0]
    table_name = table_schema.get("name")

    bucks.systest_env_setup(rest_setting, test_suite_config)

    bucks.input_client(
        config,
        test_suite,
        csv_file_path,
        interval=0.5,
        data_sets_play_mode="repeat",
        batch_size=1,
        agent_id="agent_1",
    )
'''


