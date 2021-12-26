#!/usr/bin/python3
# _*_ coding: utf-8 _*_
#
# framework for query verification tests
# rockets_run logic:
#   0. test_context setup(read in all the configs and tests), setup tables according to the config, start
#      query_execute as a process and setup pipe conn betwen rockets_run and query_execute
#   1. run pre-statements by query_execute, rockets_run create query_id automatically and send the statements
#      to query_execute one by one, query_execute execute each one got , get all the results with query_id and
#      send back to rockets_run, currently only 1 stream query as the last pre_statement is supported
#   2. inputs (put datas into proton)
#   3. run post-statements by query_execute, rockets_run create query_id automatically and send the statements
#      to query_execute one by one, query_execute execute each one got , get all the results with query_id and
#      send back to rockets_run, currently only 1 stream query as the last pre_statement is supported
#   4. inputs (put datas into proton)
#   5. Compare expect resutls with result of each statement by query_id for assert (skip means skip comparison)
#   6. all the inputs are injected by rockets_run, all the statements are done by query_execute
#   7. clean test environment, drop table, clean pipes and etc.
#
# command structure from rockets_run to query_execute
# {
#    "query_id":"10001",
#    "query_type":"stream",
#    "query": "select * from test emit stream"
# }
# query_results from query_execute to rockets_run
# {
#    "query_id": "457",
#    "query_type": "stream",
#    "query_state": "run",
#    "query_start": "2021-12-01 20:20:00",
#    "query_end": "2021-12-01 20:20:00",
#    "query_result":
#    ["('dev2', 78.30000305175781, datetime.datetime(2020, 2, 2, 20, 0), datetime.datetime(2020, 2, 2, 20, 0, 10))",
#    "('dev2', 78.30000305175781, datetime.datetime(2020, 2, 2, 20, 0), datetime.datetime(2020, 2, 2, 20, 0, 10))",
#    "('dev2', 78.30000305175781, datetime.datetime(2020, 2, 2, 20, 0), datetime.datetime(2020, 2, 2, 20, 0, 10))"
#    ]
# }
# import global_settigns

import os, sys, json, getopt, subprocess
import logging, logging.config
import time
import datetime
import random
import requests
import multiprocessing as mp
from clickhouse_driver import Client
from clickhouse_driver import errors
from requests.api import request


cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)


def rockets_env_var_get():
    proton_server = os.environ.get("PROTON_HOST")
    proton_server_native_port = os.environ.get("PROTON_NATIVE_PORT")
    proton_rest_port = os.environ.get("PROTON_REST_PORT")
    proton_rest_params = os.environ.get("PROTON_REST_PARAMS")
    proton_rest_table_ddl_path = os.environ.get("PROTON_REST_TABLE_PATH")
    proton_rest_ingest_path = os.environ.get("PROTON_REST_INGEST_PATH")
    proton_rest_query_path = os.environ.get("PROTON_REST_QUERY_PATH")
    proton_rest_health_path = os.environ.get("PROTON_REST_HEALTH_PATH")
    proton_rest_info_path = os.environ.get("PROTON_REST_INFO_PATH")

    if (
        proton_server != None
        and proton_server_native_port != None
        and proton_rest_port != None
        and proton_rest_params != None
        and proton_rest_table_ddl_path != None
        and proton_rest_ingest_path != None
        and proton_rest_query_path != None
        and proton_rest_health_path != None
        and proton_rest_info_path != None
    ):

        config = {
            "rest_setting": {
                "table_ddl_url": f"http://{proton_server}:{proton_rest_port}{proton_rest_table_ddl_path}",
                "ingest_url": f"http://{proton_server}:{proton_rest_port}{proton_rest_ingest_path}",
                "query_url": f"http://{proton_server}:{proton_rest_port}{proton_rest_query_path}",
                "health_check_url": f"http://{proton_server}:{proton_rest_port}{proton_rest_health_path}",
                "info_url": f"http://{proton_server}:{proton_rest_port}{proton_rest_info_path}",
                "prarams": proton_rest_params,
            },
            "roton_server": proton_server,
            "proton_server_native_port": proton_server_native_port,
        }
        return config
    else:
        return None


def rockets_context(config_file=None, tests_file=None, docker_compose_file=None):
    config = rockets_env_var_get()
    if config == None:
        with open(config_file) as f:
            config = json.load(f)
        logging.debug(f"rockets_context: config reading from config files: {config}")

    if config == None:
        raise Exception("No config env vars nor config file")
    # proton_server = config.get("proton_server")
    # proton_server_native_port = config.get("proton_server_native_port")

    with open(tests_file) as f:
        test_suite = json.load(f)

    # tests = test_suite.get("tests")
    (
        query_exe_parent_conn,
        query_exe_child_conn,
    ) = (
        mp.Pipe()
    )  # create the pipe for inter-process conn of rockets_run and query_execute
    # query_exe_client = mp.Process(target=query_execute, args=(config, query_exe_child_conn, query_result_list)) # Create query_exe_client process
    query_exe_client = mp.Process(
        target=query_execute, args=(config, query_exe_child_conn)
    )  # Create query_exe_client process

    rockets_context = {
        "config": config,
        "test_suite": test_suite,
        "query_exe_client": query_exe_client,
        "docker_compose_file": docker_compose_file,
        "query_exe_parent_conn": query_exe_parent_conn,
        "query_exe_child_conn": query_exe_child_conn,
    }
    ROCKETS_CONTEXT = rockets_context
    return rockets_context


def tuple_2_list(tuple):
    # transfer tuple to jason string
    _list = []
    for element in tuple:
        if isinstance(element, str):
            _list.append(element)
        else:
            _list.append(str(element))
    return _list


def kill_query(
    proton_client,
    query_2_kill,
    query_conn,
    statements_results,
    query_end_timer_start,
    query_end_timer=0,
):
    # currently only stream query kill logic is done, query_2_kill is the query_id, get the id and kill the query and recv the stream query results from query_execute
    # pay attention: the stream query must be created by the query_execute and a child_conn.send(query_results) must be done by query_execute, watch the pipe conn pairs.
    query_results = ""
    kill_sql = f"kill query where query_id = '{query_2_kill}'"
    # run the timer and then kill the query
    logging.debug(f"kill_query: datetime.now = {datetime.datetime.now()}.")
    query_end_time = query_end_timer_start + datetime.timedelta(seconds=query_end_timer)
    logging.debug(
        f"kill_query: datetime.timedelta(seconds=query_end_timer) = {query_end_time}."
    )
    if query_end_time > datetime.datetime.now():
        sleep_time_seconds = (
            query_end_timer_start
            + datetime.timedelta(seconds=query_end_timer)
            - datetime.datetime.now()
        ).seconds
    else:
        sleep_time_seconds = 0
    logging.debug(f"kill_query: sleep_time_seconds = {sleep_time_seconds}.")
    time.sleep(
        sleep_time_seconds
    )  # sleep for the query_end_timer and then kill the query.
    kill_res = proton_client.execute(kill_sql)
    logging.debug(f"kill_query: kill_sql = {kill_sql} cmd executed.")
    if len(kill_res):
        time.sleep(0.1)
        kill_res = proton_client.execute(kill_sql)
    query_conn_recv = query_conn.recv()
    query_results = json.loads(query_conn_recv)
    logging.debug(
        f"kill_query: stream query result receved from query_execute: {query_results}"
    )
    if query_results:
        statements_results.append(
            query_results
        )  # add the stream query_results collected to statements_results, only results of one query, just append


# def query_execute(config, child_conn, query_result_list):
def query_execute(config, child_conn):
    logging.basicConfig(level=logging.DEBUG, filename="rockets.log")
    # query_result_list = query_result_list
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    settings = {"max_block_size": 100000}
    query_result_str = None
    tear_down = False
    query_run_count = 100
    # max query_run_count, hard code right now, could be sent from query_walk_through (for example based on total statements no.)
    client = Client(
        host=proton_server, port=proton_server_native_port
    )  # create python client
    while ~tear_down and query_run_count > 0:
        try:

            message_recv = child_conn.recv()
            if message_recv == "tear_down":
                tear_down = True
                break

            statement_2_run = json.loads(json.dumps(message_recv))
            query_id = str(statement_2_run.get("query_id"))
            query_client = statement_2_run.get("client")
            query_type = statement_2_run.get("query_type")
            query = statement_2_run.get("query")
            query_start_time_str = str(datetime.datetime.now())
            query_end_time_str = str(datetime.datetime.now())
            element_json_str = ""
            query_result_str = ""
            query_results = {}
            query_result_list = []
            query_result_column_types = []

            # send a message to query_walk_through when each query was sent by client in query_execute threfore query_walk_through can go next
            i = 0
            # iter index
            # child_conn.send("query is executed by Client. ")
            # client = Client(host=proton_server, port=proton_server_native_port) # create python client
            query_result_iter = client.execute_iter(
                query, with_column_types=True, query_id=query_id, settings=settings
            )
            query_executed_msg = {
                "query_id": query_id,
                "status": "run",
                "timestamp": str(datetime.datetime.now()),
            }
            query_executed_msg_json = json.dumps(query_executed_msg)
            child_conn.send(query_executed_msg_json)
            for element in query_result_iter:
                logging.debug(
                    f"query_execute: element in query_result_iter in query_id: {query_id} = {element}"
                )
                # print(f'query_execute: element in query_result_iter in query_id: {query_id} = {element}')
                if isinstance(element, list) or isinstance(element, tuple):
                    element = list(element)
                    element.append({"timestamp": str(datetime.datetime.now())})
                if i == 0:
                    query_result_column_types = element
                    logging.debug(
                        f"query_execute: query_result_iter: query_result_column_types recved from proton: {element}"
                    )
                else:
                    element_list = tuple_2_list(element)
                    query_result_list.append(element_list)
                    logging.debug(
                        f"query_execute: query_result_iter: query_id: {query_id} row {i}: {query_result_list} recved from proton"
                    )
                i += 1
            query_end_time_str = str(
                datetime.datetime.now()
            )  # record query_end_time, and transfer to str
            query_results = {
                "query_id": query_id,
                "query": query,
                "query_type": query_type,
                "query_state": "run",
                "query_start": query_start_time_str,
                "query_end": query_end_time_str,
                "query_result_column_types": query_result_column_types,
                "query_result": query_result_list,
            }
            # logging.debug("rockets.py query_execute func: query_results: {} collected from query_result_iter at {}".format(query_results, datetime.datetime.now()))
            message_2_send = json.dumps(query_results)
            child_conn.send(message_2_send)
            # query_result_list = []
            query_run_count = query_run_count - 1
        except (BaseException, errors.ServerException) as error:
            if isinstance(error, errors.ServerException):
                if (
                    error.code == 394
                ):  # if the query is canceled '394' will be caught and compose the query_results and send to inputs_walk_through
                    # send the result
                    query_end_time_str = str(datetime.datetime.now())
                    query_results = {
                        "query_id": query_id,
                        "query": query,
                        "query_type": query_type,
                        "query_state": "run",
                        "query_start": query_start_time_str,
                        "query_end": query_end_time_str,
                        "query_result_column_types": query_result_column_types,
                        "query_result": query_result_list,
                    }
                    logging.debug(
                        "query_execute func: query_results: {} collected from query_result_iter at {}".format(
                            query_results, datetime.datetime.now()
                        )
                    )
                    # print("rockets.py query_execute func: query_results: {} collected from query_result_iter at {}".format(query_results, datetime.datetime.now()))
                    message_2_send = json.dumps(query_results)
                    child_conn.send(message_2_send)
                    # query_result_list = []
                    # logging.debug("rockets.py query_execute func: query_results {} from query_result_iter at sent at {}".format(query_results, datetime.datetime.now()))

                else:  # for other exception code, send the error_code as query_result back, some tests expect eception will use.
                    query_end_time_str = str(datetime.datetime.now())
                    query_results = {
                        "query_id": query_id,
                        "query": query,
                        "query_type": query_type,
                        "query_state": "exception",
                        "query_start": query_start_time_str,
                        "query_end": query_end_time_str,
                        "query_result": f"error_code:{error.code}",
                    }
                    logging.debug(
                        "query_execute: db exception, none-cancel query_results: {}".format(
                            query_results
                        )
                    )
                    message_2_send = json.dumps(query_results)
                    child_conn.send(message_2_send)
                    # query_result_list = []
                # client.disconnect()
                query_run_count = query_run_count - 1
            else:
                # print("query_execute: run in except _else -none db error in query_execute", error)
                query_results.append(
                    {
                        "query_id": query_id,
                        "query": query,
                        "query_type": query_type,
                        "query_state": "exception",
                        "query_start": query_start_time_str,
                        "query_end": query_end_time_str,
                        "query_result": "error_code:10000",
                    }
                )  # if it's not db excelption, send 10000 as error_code
                message_2_send = json.dumps(query_results)
                # client.disconnect()
                child_conn.send(message_2_send)
                query_run_count = query_run_count - 1
    # tear down, when "tear_down" is received or the query_count is hit
    client.disconnect()
    child_conn.send("tear_down")


def query_walk_through(proton_client, statements, query_conn):
    statement_id_run = 0
    querys_results = []
    query_results_json_str = ""
    stream_query_id = None
    query_end_timer = 0
    query_id = random.randint(1, 10000)  # unique query id
    while statement_id_run < len(statements):
        query_results = {}
        query_executed_msg = {}
        client = statements[statement_id_run].get("client")
        query = statements[statement_id_run].get("query")
        query_type = statements[statement_id_run].get("query_type")
        query_end_timer = statements[statement_id_run].get("query_end_timer")
        logging.debug(
            f"query_walk_through: query_id = {query_id}: query_end_timer = {query_end_timer}."
        )
        if query_end_timer == None:
            query_end_timer = 0
        statement_2_run = {
            "query_id": query_id,
            "client": client,
            "query_type": query_type,
            "query": query,
        }
        query_conn.send(statement_2_run)
        logging.debug(
            f"query_walk_through: query_id = {query_id}: {query}was sent to query_execute."
        )
        if query_type == "stream" or query_type == "lastx":
            query_conn_recv = (
                query_conn.recv()
            )  ##receive the "query_sent" from query_execute and go next, and due to stream query result to be received in inputs_walk_through, don't need recv query_results.
            query_executed_msg = json.loads(query_conn_recv)
            query_executed_timestamp = query_executed_msg.get("timestamp")
            query_end_timer_start = datetime.datetime.fromisoformat(
                query_executed_timestamp
            )
            logging.debug(
                f"query_walk_through: query_id = {query_id}: query_end_timer_start: {query_end_timer_start}."
            )
            statement_id_run += 1
            stream_query_id = query_id
        else:
            query_conn_recv = (
                query_conn.recv()
            )  # receive the "query_sent" from query_execute for query_execute comfirmation and go next
            query_conn_recv = query_conn.recv()  # receive the query_result
            query_results = json.loads(json.dumps(query_conn_recv))
            statement_id_run += 1
            querys_results.append(query_results)
        query_id += 1
        # time.sleep(1) # wait the query_execute execute the stream command
    logging.debug(
        f"query_walk_through: query_id = {query_id}: query_end_timer = {query_end_timer} before query_walk_through_end."
    )
    logging.debug("query_walk_through end.")
    return [querys_results, stream_query_id, query_end_timer_start, query_end_timer]


def input_walk_through_pyclient(proton_client, inputs, table_schema):
    input_results = []
    # walk through inputs
    columns = table_schema.get("columns")
    table_name = table_schema.get("name")
    table_columns = ""
    for element in columns:
        table_columns = table_columns + element.get("name") + ","
    table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"

    if len(inputs) > 0:
        for batch in inputs:
            batch_str = " "
            for row in batch:
                row_str = " "
                for field in row:
                    # print("input_walk_through: field:", field)
                    if isinstance(field, str):
                        field.replace('"', '//"')  # proton does
                    row_str = (
                        row_str + "'" + str(field) + "'" + ","
                    )  # python client does not support "", so put ' here
                row_str = "(" + row_str[: len(row_str) - 1] + ")"
                batch_str = batch_str + row_str + ","
            batch_str = batch_str[: len(batch_str) - 1]
            input_sql = (
                f"insert into {table_name} {table_columns_str} values {batch_str}"
            )
            input_result = proton_client.execute(input_sql)
            logging.debug(
                "input_walk_through_pyclient: {} done at {}.".format(
                    input_sql, datetime.datetime.now()
                )
            )
            # time.sleep(1)
            input_results.append(input_result)
        time.sleep(1)  # wait 1s for data inputs completed.
    return input_results


def input_batch_rest(input_url, input_batch, table_schema):
    # todo: complete the input by rest
    input_batch_record = {}
    table_name = table_schema.get("name")
    input_rest_columns = []
    input_rest_body_data = []
    input_rest_body = {"columns": input_rest_columns, "data": input_rest_body_data}
    for element in table_schema.get("columns"):
        input_rest_columns.append(element.get("name"))
    for row in input_batch:
        input_rest_body_data.append(row)
    input_rest_body = json.dumps(input_rest_body)
    input_url = f"{input_url}/{table_name}"
    res = requests.post(input_url, data=input_rest_body)

    assert res.status_code == 200
    input_batch_record["input_batch"] = input_rest_body_data
    input_batch_record["timestamp"] = str(datetime.datetime.now())

    """
    if res.status_code != 200:
        logging.debug(f"table input rest access failed, status code={res.status_code}") 
        raise Exception(f"table input rest access failed, status code={res.status_code}")
    else:
        input_batch_record["input_batch"] =input_rest_body_data
        input_batch_record["timestamp"] = str(datetime.datetime.now())
        #logging.debug("input_rest: input_batch {} is inserted".format(input_rest_body_data)) 
    """
    return input_batch_record


def input_walk_through_rest(
    rest_setting,
    inputs,
    table_schema,
    wait_before_inputs=1,
    sleep_after_inputs=1.5,  # stable set wait_before_inputs=1, sleep_after_inputs=1.5
):
    wait_before_inputs = wait_before_inputs  # the seconds sleep before inputs starts to ensure the query is run on proton.
    sleep_after_inputs = sleep_after_inputs  # the seconds sleep after evary inputs of a case to ensure the stream query result was emmited by proton and received by the query execute
    time.sleep(wait_before_inputs)
    input_url = rest_setting.get("ingest_url")
    inputs_record = []
    for batch in inputs:
        input_batch_record = input_batch_rest(input_url, batch, table_schema)
        inputs_record.append(input_batch_record)
        # time.sleep(0.5)
    time.sleep(sleep_after_inputs)
    return inputs_record


def drop_table_if_exist_rest(table_ddl_url, table_name):
    res = requests.get(table_ddl_url)
    if res.status_code == 200:
        res_json = res.json()
        table_list = res_json.get("data")
        if table_list:
            for element in table_list:
                if element.get("name") == table_name:
                    res = requests.delete(f"{table_ddl_url}/{table_name}")
                    if res.status_code != 200:
                        raise Exception(
                            f"table drop rest access failed, status code={res.status_code}"
                        )
                    else:
                        time.sleep(1)  # sleep to wait the table drop completed.
                        logging.info(
                            "env_setup: table {} is dropped".format(table_name)
                        )
        else:
            logging.info(f"env_setup: table {table_name} does not exit")
    else:
        raise Exception(f"table list rest acces failed, status code={res.status_code}")


def table_exist(table_ddl_url, table_name):
    res = requests.get(table_ddl_url)
    logging.debug(f"table_exist: res.status_code = {res.status_code}")
    if res.status_code == 200:
        res_json = res.json()
        table_list = res_json.get("data")
        if len(table_list) > 0:
            for element in table_list:
                element_name = element.get("name")
                # logging.debug(f"table_exist: element_name = {element_name}, table_name = {table_name}")
                if element_name == table_name:
                    logging.debug(f"table_exist: table {table_name} exists.")
                    return True
            return False
        else:
            logging.debug("table_exist: False")
            return False


def create_table_rest(table_ddl_url, table_schema):
    # logging.debug(f"create_table_rest: table_ddl_url = {table_ddl_url}, table_schema = {table_schema}")
    table_name = table_schema.get("name")
    res = requests.post(
        table_ddl_url, data=json.dumps(table_schema)
    )  # create the table w/ table schema
    if res.status_code == 200:
        logging.debug(f"table {table_name} create_rest is called successfully.")
    else:
        raise Exception(
            f"table create rest access failed, status code={res.status_code}"
        )
    # show tables and check if table_name creates

    create_table_time_out = 5  # set how many times wait and list table to check if table creation completed.
    while create_table_time_out > 0:
        if table_exist(table_ddl_url, table_name):
            print(f"table {table_name} is created sussufully.")
            break
        else:
            time.sleep(2)
            # res = requests.post(table_ddl_url, data=json.dumps(table_schema)) #currently the health check rest is not accurate, retry here and remove later
        create_table_time_out -= 1
    # time.sleep(1) # wait the table creation completed


def compose_up(compose_file_path):
    print(f"compose_up: compose_file_path = {compose_file_path}")
    try:
        cmd = f"docker-compose -f {compose_file_path} up -d"
        logging.debug(f"compose_up: cmd = {cmd}")
        res = subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError) as Error:
        return False


def env_health_check(health_check_url):
    try:
        logging.debug(f"env_health_check: health_check_url = {health_check_url}")
        res = requests.get(health_check_url)
        if res.status_code == 200:
            return True
        else:
            return False
    except (BaseException):
        return False


def env_setup(rest_setting, test_suite_config, env_compose_file, proton_ci_mode):
    ci_mode = proton_ci_mode
    logging.info(f"env_setup: ci_mode = {ci_mode}")
    logging.debug(f"env_setup: rest_setting = {rest_setting}")
    health_url = rest_setting.get("health_check_url")
    logging.debug(f"env_setup: health_url = {health_url}")
    if ci_mode == "local":
        env_docker_compose_res = True
        logging.info(f"Bypass docker compose up.")
    else:
        env_docker_compose_res = compose_up(env_compose_file)
        logging.info(f"docker compose up...")
    logging.debug(f"env_setup: env_docker_compose_res: {env_docker_compose_res}")
    env_health_check_res = env_health_check(health_url)
    logging.info(f"env_setup: env_health_check_res: {env_health_check_res}")
    if env_docker_compose_res:
        retry = 5
        while env_health_check_res == False and retry > 0:
            time.sleep(2)
            env_health_check_res = env_health_check(health_url)
            logging.debug(f"env_setup: retry = {retry}")
            retry -= 1

        if env_health_check_res == False:
            raise Exception("Env health check failure.")
    else:
        raise Exception("Env docker compose up failure.")
    if ci_mode == "github":
        time.sleep(
            5
        )  # health check rest is not accurate, wait after docker compsoe up under github mode, remove later when it's fixed.
    table_schema = test_suite_config.get("table_schema")
    table_ddl_url = rest_setting.get("table_ddl_url")
    params = rest_setting.get("params")
    table_name = table_schema.get("name")
    if table_name != None:
        drop_table_if_exist_rest(table_ddl_url, table_name)
    create_table_rest(table_ddl_url, table_schema)
    return


# @pytest.fixture(scope="module")
def rockets_run(test_context):
    logging.info("rockets_run starts......")
    docker_compose_file = test_context.get("docker_compose_file")
    config = test_context.get("config")
    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    q_exec_client = test_context.get("query_exe_client")
    query_conn = test_context.get("query_exe_parent_conn")
    q_exec_client_conn = test_context.get("query_exe_child_conn")
    q_exec_client.start()
    test_suite = test_context.get("test_suite")
    test_suite_config = test_suite.get("test_suite_config")
    table_schema = test_suite_config.get("table_schema")
    tests = test_suite.get("tests")
    tests_2_run = test_suite_config.get("tests_2_run")
    test_run_list = []
    proton_ci_mode = os.getenv("PROTON_CI_MODE", "Github")
    logging.info(f"rockets_run: proton_ci_mode = {proton_ci_mode}")

    if tests_2_run == None:  # if tests_2_run is not set, run all tests.
        test_run_list = tests
    else:  # if tests_2_run is set in test_suite_config, run the id list.
        logging.debug(f"rockets_run: tests_2_run is configured as {tests_2_run}")
        ids_2_run = tests_2_run.get("ids_2_run")
        if len(ids_2_run) != 0:
            if ids_2_run[0] == "all":
                test_run_list = tests
            else:
                # tests = []
                for test in tests:
                    if test.get("id") in ids_2_run:
                        test_run_list.append(test)
                # logging.debug(f"rockets_run: {len(test_run_list)} tests: {test_run_list} to be run based on tests_2_run: {tests_2_run} configured")
        else:
            print("rockets_run: test_suite_config is misconfigured. ")
            return []
        logging.debug(f"rockets_run: tests_2_run is configured as {tests_2_run}")

    env_setup(rest_setting, test_suite_config, docker_compose_file, proton_ci_mode)
    logging.info("rockets_run env_etup done")
    # logging.info(f"test_run_list = {test_run_list}")

    test_id_run = 0
    test_sets = []
    try:
        client = Client(host=proton_server, port=proton_server_native_port)
        while test_id_run < len(test_run_list):
            query_2_kill = ""
            query_end_timer_start = None
            query_end_timer = 0
            statements_results = []
            inputs_record = []
            # read pre_statements, send to q_exe_client, and get result, put into test results[]
            test_id = test_run_list[test_id_run].get("id")
            pre_statements = test_run_list[test_id_run].get("pre_statements")
            test_name = test_run_list[test_id_run].get("name")
            inputs = test_run_list[test_id_run].get("inputs")
            post_statements = test_run_list[test_id_run].get("post_statements")
            expected_results = test_run_list[test_id_run].get("expected_results")

            if pre_statements:
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} pre_statement start"
                )
                query_walk_through_res = query_walk_through(
                    client, pre_statements, query_conn
                )  # pre_statements walk through
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} pre_statements query_walk_through_res = {query_walk_through_res}"
                )
                (
                    statement_result_from_query_execute,
                    query_2_kill,
                    query_end_timer_start,
                    query_end_timer,
                ) = query_walk_through_res
                # statement_result_from_query_execute = query_walk_through_res[0]
                if statement_result_from_query_execute:
                    for element in statement_result_from_query_execute:
                        statements_results.append(element)
                # query_2_kill = query_walk_through_res[1]
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} pre_statement done"
                )
            if inputs:
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} input_walk_through start"
                )
                # time.sleep(0.5) # sleep 0.5 sec to ensure the
                # input_walk_through(client, inputs, table_schema) #inputs walk through via python client
                inputs_record = input_walk_through_rest(
                    rest_setting, inputs, table_schema
                )  # inputs walk through rest_client
                # input_walk_through_res = input_walk_through(client, inputs, table_schema, query_2_kill, query_conn) #inputs walk through
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} input_walk_through done"
                )
                # time.sleep(0.5) #wait for the data inputs done.

            if query_2_kill:
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} kill_query after pre_statement start."
                )
                kill_query(
                    client,
                    query_2_kill,
                    query_conn,
                    statements_results,
                    query_end_timer_start,
                    query_end_timer,
                )
                query_2_kill = ""
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} kill_query after pre_statement done."
                )
            if post_statements:
                input_walk_through_rest(
                    rest_setting, inputs, table_schema
                )  # inputs walk through rest_client
                statement_result_from_query_execute = query_walk_through_res[0]
                if statement_result_from_query_execute:
                    for element in statement_result_from_query_execute:
                        statements_results.append(element)

                query_2_kill = query_walk_through_res[1]
                logging.info(
                    f"rockets_run: {test_id_run}, test_id = {test_id} post_statement done."
                )
            if query_2_kill:
                kill_query(client, query_2_kill, query_conn, statements_results)
                query_2_kill = ""

            test_sets.append(
                {
                    "test_id_run": test_id_run,
                    "test_id": test_id,
                    "test_name": test_name,
                    "inputs_record": inputs_record,
                    "expected_results": expected_results,
                    "statements_results": statements_results,
                }
            )
            test_id_run += 1

    except (EOFError) as error:
        logging.info("exception:", error)
    finally:
        TESTS_QUERY_RESULTS = test_sets
        query_conn.send("tear_down")
        query_conn.recv()
        query_conn.close()
        q_exec_client_conn.close()
        q_exec_client.terminate()
        client.disconnect()
        return test_sets


if __name__ == "__main__":
    # cur_run_path = os.getcwd()
    # cur_run_path_parent = os.path.dirname(cur_run_path)
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

    argv = sys.argv[1:]  # get -d to specify the test_sutie path
    try:
        opts, args = getopt.getopt(argv, "d:")
    except:
        print("Error")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-d"]:
            test_suite_path = arg
    if test_suite_path == None:
        print("No test suite directory specificed by -d, exit.")
        sys.exit(0)
    # config_file = f"{cul_run_path}/configs/config.json"
    config_file = f"{test_suite_path}/configs/config.json"
    tests_file = f"{test_suite_path}/tests.json"
    docker_compose_file = f"{test_suite_path}/configs/docker-compose.yaml"

    if os.path.exists(tests_file):
        rockets_context = rockets_context(
            config_file, tests_file, docker_compose_file
        )  # need to have config env vars/config.json and test.json when run rockets.py as a test debug tooling.
        test_sets = rockets_run(rockets_context)
        # output the test_sets one by one
        logging.info("main: ouput test_sets......")
        for test_set in test_sets:
            test_set_json = json.dumps(test_set)
            logging.info(f"main: test_set from rockets_run: {test_set_json} \n\n")
    else:
        print("No tests.json exists under test suite folder.")

