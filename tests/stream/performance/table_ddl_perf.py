from ast import Pass
import os, sys, getopt, json, random
from re import sub
import logging, logging.config
from clickhouse_driver import Client
from clickhouse_driver import errors
import csv
import datetime
import time
import requests
import multiprocessing as mp
import uuid
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from helpers import rockets
from helpers.rockets import env_setup
from helpers.rockets import table_exist
from helpers.rockets import create_table_rest
from helpers.rockets import find_schema
from helpers.rockets import drop_table_if_exist_rest
from helpers.rockets import find_table_reset_in_table_schemas
from helpers.rockets import reset_tables_of_test_inputs
from helpers.rockets import TABLE_CREATE_RECORDS
from helpers.rockets import TABLE_DROP_RECORDS
from helpers.rockets import VIEW_CREATE_RECORDS

from helpers.bucks import query_walk_through
from helpers.bucks import env_var_get

from helpers.bucks import reset_tables_of_test_inputs


cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)


#table_unavailable_count = mp.Value("b", 0)
table_unavailable_count = 0


def json_load_from_file(file_path):
    if os.path.exists(file_path):
        with open(file_path) as json_file:
            json_context = json.load(json_file)
        # logging.debug(f"main: github_gdi_context = {github_gdi_context}")
        return json_context
    else:
        return None


def context_load(json_context):
    if json_context == None:
        logger.info("no context gotten, exit")
        return None
    else:
        for (
            key
        ) in (
            json_context
        ):  # read the context and create a var by set the name to key and value to context[key]
            names = locals()
            exec("{} = {}".format(key, json_context[key]))
            var_name = names.get(key)


def clean_all_tables(proton_server):
    pyclient = Client(proton_server)
    tables_res = pyclient.execute("show tables")
    if len(tables_res) == 0:
        return True
    for name in tables_res:
        sql = f"drop table if exists {name[0]}"
        
        pyclient.execute(sql)
    while len(tables_res) != 0:
        time.sleep(0.01)
        tables_res = pyclient.execute("show tables")
        
    logger.debug(f"tables_res = {tables_res}")
    return True


def get_time_spent(
    setup_func, setup_func_args, start_func, start_func_args, complete_func, complete_func_args, loop_count
):
    if loop_count <= 0:
        logger.info(f"bad loop_count = {loop_count}")
        return None
    time_spent_list = []
    count = 0
    while count < loop_count:
        res_setup = setup_func(*setup_func_args)
        logger.debug(f"res_setup = {res_setup}")
        start = datetime.datetime.now()
        res_start = start_func(*start_func_args)
        logger.debug(f"res_start.json.data = {res_start.json()}")
        res_complete = complete_func(*complete_func_args)
        logger.debug(f"res_complete = {res_complete}")

        while not res_complete:
            time.sleep(0.01)
        complete = datetime.datetime.now()
        time_spent = complete - start
        time_spent_ms = time_spent.total_seconds() * 1000
        time_spent_list.append(
            {"func_name": start_func.__name__, "time_spent_ms": time_spent_ms}
        )
        count += 1

    time_spent_total_ms = 0
    avg_time_spent_ms = 0
    count = 0
    for item in time_spent_list:
        time_spent_total_ms += item.get("time_spent_ms")
        count += 1
    avg_time_spent_ms = time_spent_total_ms / count
    return {
        "time_spent_total": time_spent_total_ms,
        "count": count,
        "avg_time_spent_ms": avg_time_spent_ms,
    }


def query_run(proton_server, sql):
    table_querable = False
    pyclient = Client(proton_server)
    while  table_querable == False:
        try:
            logger.debug(f"sql = {sql}")
            res = pyclient.execute(sql, query_id="101")
        except (BaseException, errors.ServerException) as error:
            # pyclient.disconnect()
            time.sleep(0.002)
            # pyclient.disconnect()
            global table_unavailable_count
            table_unavailable_count += 1
        table_querable = True


def stream_query(proton_server, sql):

    t = threading.Thread(
        target=query_run,
        args=(
            proton_server,
            sql,
        ),
    )
    count = table_unavailable_count
    t.start()
    time.sleep(0.01)
    while count != table_unavailable_count:
        time.sleep(0.01)
        count += 1
    client = Client(proton_server)
    client.execute("kill query where query_id='101'")
    return True


if __name__ == "__main__":
    cur_file_path = os.path.dirname(os.path.abspath(__file__))
    cur_file_path_parent = os.path.dirname(cur_file_path)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    argv = sys.argv[1:]  # get -d to specify the test_sutie path
    try:
        opts, args = getopt.getopt(argv, "d:")
    except (BaseException) as error:
        logging.info(error)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-d"]:
            context_file_path = arg + "table_ddl_perf.json"
    if context_file_path == None:
        logging.info("No config directory specificed by -d, exit.")
        sys.exit(1)

    json_context = json_load_from_file(context_file_path)
    # context_load(context)

    if json_context == None:
        logging.info("no context gotten, exit")
        sys.exit(1)
    else:
        for (
            key
        ) in (
            json_context
        ):  # read the context and create a var by set the name to key and value to context[key]
            names = locals()
            exec("{} = {}".format(key, json_context[key]))
            var_name = names.get(key)
            logging.debug(f"key in context: {key} = {var_name}")

    for key in rest_setting:
        names = locals()
        exec("{} = {}".format(key, rest_setting[key]))
        var_name = names.get(key)
        logging.debug(f"key in context: {key} = {var_name}")

    client = Client(proton_server)

    #res = clean_all_tables(proton_server)
    clean_all_tables_args = (proton_server,)
    create_table_rest_args = (table_ddl_url, table_schema)
    stream_query_args = (proton_server, 'select * from test')
    res = get_time_spent(clean_all_tables, clean_all_tables_args, create_table_rest, create_table_rest_args, stream_query, stream_query_args, 10)
    print(res)

    
