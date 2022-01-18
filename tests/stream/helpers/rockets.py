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
    )  # create the pipe for inter-process conn of rockets_run and query_execute, control path
    # query_exe_client = mp.Process(target=query_execute, args=(config, query_exe_child_conn, query_result_list)) # Create query_exe_client process
    # query_exe_queue = mp.Queue() #control path queue for query statements, rockets_run pushes statements into the queue.
    query_results_queue = (
        mp.Queue()
    )  # data path queue for query results, query_execute and threads created by query_execute process pushed query results into this queue.

    # query_exe_client = mp.Process(target=query_execute_new, args=(config, query_exe_queue, query_results_queue))

    query_exe_client = mp.Process(
        target=query_execute,
        args=(config, query_exe_child_conn, query_results_queue),
    )  # Create query_exe_client process

    rockets_context = {
        "config": config,
        "test_suite": test_suite,
        "query_exe_client": query_exe_client,
        "docker_compose_file": docker_compose_file,
        "query_exe_parent_conn": query_exe_parent_conn,
        "query_exe_child_conn": query_exe_child_conn,
        "query_results_queue": query_results_queue,
    }
    """
    rockets_context = {
        "config": config,
        "test_suite": test_suite,
        "query_exe_client": query_exe_client,
        "docker_compose_file": docker_compose_file,
        "query_exe_queue": query_exe_queue,
        "query_results_queue": query_results_queue
    }
    """
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


def kill_query(proton_client, query_2_kill):
    # currently only stream query kill logic is done, query_2_kill is the query_id, get the id and kill the query and recv the stream query results from query_execute

    kill_sql = f"kill query where query_id = '{query_2_kill}'"
    # run the timer and then kill the query
    logging.debug(
        f"kill_query: datetime.now = {datetime.datetime.now()}, kill_sql = {kill_sql}."
    )
    kill_res = proton_client.execute(kill_sql)
    logging.debug(
        f"kill_query: kill_sql = {kill_sql} cmd executed, kill_res = {kill_res}"
    )
    while len(kill_res):
        time.sleep(0.2)
        kill_res = proton_client.execute(kill_sql)
        logging.debug(f"kill_query: kill_res = {kill_res}")


def query_run_py(
    statement_2_run, settings, query_results_queue=None, config=None, pyclient=None
):
    try:
        if pyclient == None:
            proton_server = config.get("proton_server")
            proton_server_native_port = config.get("proton_server_native_port")
            settings = {"max_block_size": 100000}
            pyclient = Client(
                host=proton_server, port=proton_server_native_port
            )  # create python client
            CLEAN_CLIENT = True

        query = statement_2_run.get("query")
        query_id = str(statement_2_run.get("query_id"))
        query_type = statement_2_run.get("query_type")
        run_mode = statement_2_run.get("run_mode")
        query_start_time_str = str(datetime.datetime.now())
        query_end_time_str = str(datetime.datetime.now())
        element_json_str = ""
        query_result_str = ""
        query_result_column_types = []
        query_result_list = []

        logging.debug(
            f"query_run_py: query_id = {query_id}, query = {query} to be execute........."
        )
        query_result_iter = pyclient.execute_iter(
            query, with_column_types=True, query_id=query_id, settings=settings
        )

        logging.debug(
            f"query_run_py: query_run_py: query_id = {query_id}, query = {query} executed......"
        )

        i = 0
        for element in query_result_iter:
            logging.debug(
                f"query_execute: element in query_result_iter in query_id: {query_id} = {element}"
            )

            if isinstance(element, list) or isinstance(element, tuple):
                element = list(element)
                element.append({"timestamp": str(datetime.datetime.now())})

            if i == 0:
                query_result_column_types = element

            else:
                element_list = tuple_2_list(element)
                query_result_list.append(element_list)
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
        logging.debug(f"query_run_py: query_results of query={query} = {query_results}")

        
        if query_results_queue != None:
            message_2_send = json.dumps(query_results)
            query_results_queue.put(message_2_send)
        
        if run_mode == "process" or query_type == "stream":
            pyclient.disconnect()        

    except (errors.ServerException) as error:
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
                message_2_send = json.dumps(query_results)
                if query_results_queue != None:
                    query_results_queue.put(message_2_send)
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
                    "query_run_py: db exception, none-cancel query_results: {}".format(
                        query_results
                    )
                )
                message_2_send = json.dumps(query_results)
                if query_results_queue != None:
                    query_results_queue.put(message_2_send)

                # query_result_list = []
                # client.disconnect()
        else:
            query_results = {
                "query_id": query_id,
                "query": query,
                "query_type": query_type,
                "query_state": "exception",
                "query_start": query_start_time_str,
                "query_end": query_end_time_str,
                "query_result": "error_code:10000",
            }
            # if it's not db excelption, send 10000 as error_code
            message_2_send = json.dumps(query_results)
            if query_results_queue != None:
                query_results_queue.put(message_2_send)

        if run_mode == "process" or query_type == "stream":
            pyclient.disconnect()  

    finally:

        logging.debug(f"query_run_py: query_id = {query_id}, query={query}, query_results = {query_results}")
        return query_results


def query_execute(config, child_conn, query_results_queue):
    logging.basicConfig(level=logging.DEBUG, filename="rockets.log")
    # query_result_list = query_result_list

    logging.debug(f"query_execute starts...")

    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    settings = {"max_block_size": 100000}
    query_result_str = None
    tear_down = False
    query_run_count = 1000  # limit the query run count to 1000,
    query_result_list = []
    # max query_run_count, hard code right now, could be sent from query_walk_through (for example based on total statements no.)
    client = Client(
        host=proton_server, port=proton_server_native_port
    )  # create python client

    i = 0  # query
    query_procs = []  # a list of query_run_py processes started for queries of one case
    auto_terminate_queries = []
    while (not tear_down) and query_run_count > 0:
        try:

            query_proc = None
           
            logging.debug(
                f"query_execute: tear_down = {tear_down}, query_run_count = {query_run_count}, wait for message from rockets_run......"
            )
            message_recv = child_conn.recv()
            logging.debug(f"query_execute: message_recv = {message_recv}")

            if message_recv == "tear_down":
                tear_down = True
                break

            elif message_recv == "test_steps_done":
                logging.debug(
                    f"query_execute: test_steps_done received @ {datetime.datetime.now()}, query_procs={query_procs}"
                )
                retry = 1000  # if one case run out 600s, all the processes running the streaming queries will be killed, be careful about this timeout, if some case do need run longer time, tune this setting!
                num_of_procs_2_end = len(query_procs)
                i = 0
                while i < num_of_procs_2_end and retry > 0:
                    # for proc in query_procs:
                    for proc in query_procs:
                        process = proc.get("process")
                        query_id = proc.get("query_id")
                        exitcode = process.exitcode
                        terminate = proc.get("terminate")
                        query_end_timer = proc.get("query_end_timer")
                        logging.debug(
                            f"query_execute: query_procs pop, process = {process}, exitcode = {exitcode}"
                        )

                        if process.exitcode != None:
                            i += 1
                        # else:
                        elif terminate == "auto":
                            if query_end_timer != None:
                                time.sleep(int(query_end_timer))
                            kill_query(client, query_id)
                        logging.debug(
                            f"query_execute: query_procs = {query_procs} after trying to remove"
                        )
                    time.sleep(0.1)

                    retry = retry - 1

                if retry == 0:  # terminate all the query process after retry timeout
                    for proc in query_procs:
                        process = proc.get("process")
                        exitcode = process.exitcode
                        if process.exitcode == None:
                            process.terminate()
                            query = proc.get("query")
                            logging.debug(
                                f"query_execute: process={process}, query={query} is terminated after retry timeout."
                            )
                query_procs = (
                    []
                )  # only when all the proccesses are terminated, query_procs list could be reset.

                message_2_send = "case_result_done"
                # query_exe_queue.put(message_2_send)
                child_conn.send(message_2_send)
                query_run_count -= 1
            # elif message_recv == "case_result_done":
            # message_2_send = "case_result_done"
            # query_exe_queue.put(message_recv)
            # query_run_count -= 1
            else:
                statement_2_run = json.loads(json.dumps(message_recv))
                logging.debug(f"query_execute: statement_2_run = {statement_2_run}")
                query_id = str(statement_2_run.get("query_id"))
                query_client = statement_2_run.get("client")
                query_type = statement_2_run.get("query_type")
                terminate = statement_2_run.get("terminate")
                if terminate == "auto":
                    auto_terminate_queries.append(statement_2_run)
                run_mode = statement_2_run.get("run_mode")

                query = statement_2_run.get("query")
                query_end_timer = statement_2_run.get("query_end_timer")
                query_start_time_str = str(datetime.datetime.now())
                query_end_time_str = str(datetime.datetime.now())

                if run_mode == "process" or query_type == "stream":
                    query_run_args = (
                        statement_2_run,
                        settings,
                        query_results_queue,
                        config,
                    )
                    query_proc = mp.Process(target=query_run_py, args=query_run_args)
                    query_proc.start()

                    logging.debug(
                        f"query_execute: start a proc for query = {query}, query_run_args = {query_run_args}, query_proc.pid = {query_proc.pid}"
                    )

                else:
                    logging.debug(f"query_execute: query_run_py run local...")
                    query_results = query_run_py(
                        statement_2_run,
                        settings,
                        query_results_queue=None,
                        config=None,
                        pyclient=client,
                    )

                    message_2_send = json.dumps(query_results)
                    query_results_queue.put(message_2_send)
                    logging.debug(
                        f"query_execute: query_run_py run local, query_id = {query_id}, query={query}, message_2_send = {query_results} pushed to query_results_queue "
                    )
                    time.sleep(0.2) #wait for the queue push completed, if no sleep the rockets_rum process got the results_done message too fast and then go to next case, the reulsts in queue will be lost, todo: put a beacon message to indicate the messages of cases done

                query_run_count = query_run_count - 1
        except (BaseException, errors.ServerException) as error:
            logging.debug(f"query_execute: error = {error}")
            if isinstance(error, errors.ServerException):
                query_end_time_str = str(datetime.datetime.now())
                query_results = {
                    "query_id": query_id,
                    "query": query,
                    "query_type": query_type,
                    "query_state": "run",
                    "query_start": query_start_time_str,
                    "query_end": query_end_time_str,
                    "query_result": f"error_code:{error.code}",
                }
                logging.debug(
                    "query_execute: db exception, none-cancel query_results: {}".format(
                        query_results
                    )
                )
            else:
                query_results = {
                    "query_id": query_id,
                    "query": query,
                    "query_type": query_type,
                    "query_state": "exception",
                    "query_start": query_start_time_str,
                    "query_end": query_end_time_str,
                    "query_result": "error_code:10000",
                }  # if it's not db excelption, send 10000 as error_code

            message_2_send = json.dumps(query_results)
            query_results_queue.put(message_2_send)
            query_run_count = query_run_count - 1
        finally:
            if query_proc != None:
                query_procs.append(
                    {
                        "process": query_proc,
                        "terminate": terminate,
                        "query_end_timer": query_end_timer,
                        "query_id": query_id,
                        "query": query,
                    }
                )  # put every query_run process into array for case_done check
    if query_run_count == 0:
        logging.debug(
            "Super, 1000 queries hit by a single test suite, we are in great time, by James @ Jan 10, 2022!"
        )
    if len(query_procs) != 0:
        for proc in query_procs:
            process = proc.get("process")
            process.terminate()
            process.join()
    client.disconnect()
    # query_exe_queue.put("tear_down_done")
    child_conn.send("tear_down_done")
    logging.debug(f"query_execute: tear_down completed and end")


def query_walk_through(statements, query_conn):
    logging.debug(f"query_walk_through: start..., statements = {statements}.")
    statement_id_run = 0
    querys_results = []
    query_results_json_str = ""
    stream_query_id = "False"
    query_end_timer = 0
    while statement_id_run < len(statements):
        query_results = {}
        query_executed_msg = {}
        statement = statements[statement_id_run]
        client = statement.get("client")
        wait = statement.get("wait")
        query_id = statement.get("query_id")
        query_type = statement.get("query_type")
        terminate = statement.get("terminate")
        if query_id == None:
            query_id = random.randint(
                1, 10000
            )  # unique query id, if no query_id specified in tests.json
            statement["query_id"] = query_id

        if query_type == "stream" and terminate == None:
            statement[
                "terminate"
            ] = "auto"  # for stream query, by default auto-terminate

        # query = statements[statement_id_run].get("query")
        # query_type = statements[statement_id_run].get("query_type")
        # query_end_timer = statements[statement_id_run].get("query_end_timer")
        logging.debug(f"query_walk_through: statement = {statement}.")
        if query_end_timer == None:
            query_end_timer = 0

        # query_exe_queue.put(statement)
        query_conn.send(statement)
        logging.debug(
            # f"query_walk_through: statement query_id = {query_id} was pushed into query_exe_queue."
            f"query_walk_through: statement query_id = {query_id} was send to query_execute."
        )

        if isinstance(wait, dict):  # if wait for a specific query done
            print()  # todo: check the query_id and implement the logic to notify the query_execute that this query need to be done after the query to be wait done and implement the wait logic in query_execute_new
        elif str(wait).isdigit():  # if wait for x seconds and then execute the query
            time.sleep(wait)

        statement_id_run += 1
        # time.sleep(1) # wait the query_execute execute the stream command

    logging.debug(f"query_walk_through: end... stream_query_id = {stream_query_id}")
    return querys_results


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
    logging.debug(f"input_batch_rest: input_url = {input_url}, input_rest_body = {input_rest_body}")
    
    res = requests.post(input_url, data=input_rest_body)
    logging.debug(f"input_batch_rest: response of input_batch_rest request res = {res}")
    
    

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

    for batch in inputs:
        table_name = batch.get("table_name")
        table_schema = find_schema(table_name, table_schemas)
        if table_schema != None:
            # table_schema.pop("type")
            logging.debug(f"input_walk_through_rest: table_schema = {table_schema}")
            batch_sleep_before_input = batch.get("sleep")
            if batch_sleep_before_input != None:
                time.sleep(int(batch_sleep_before_input))
            input_batch_record = input_batch_rest(input_url, batch, table_schema)
            inputs_record.append(input_batch_record)
        else:
            logging.debug(
                f"input_walk_through_rest: table_schema of table name {table_schema} not founded in table schemas {table_schemas}"
            )
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
    logging.debug(f"table_exist: table_ddl_url = {table_ddl_url}, table_name = {table_name}")
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
    type = table_schema.get("type")
    if type != None: table_schema.pop("type")  # type is not legal key/value for rest api
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
            logging.debug(f"table {table_name} is created sussufully.")
            break
        else:
            time.sleep(2)
            # res = requests.post(table_ddl_url, data=json.dumps(table_schema)) #currently the health check rest is not accurate, retry here and remove later
        create_table_time_out -= 1
    # time.sleep(1) # wait the table creation completed


def compose_up(compose_file_path):
    logging.debug(f"compose_up: compose_file_path = {compose_file_path}")
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


def systest_env_setup(
    rest_setting, test_suite_config
):  # create talbes according to test_suite_config
    # check the test env, if table w/ table_name is found, drop it
    # create table w/ table_name and table_schema to get the  table for query verification ready
    table_ddl_url = rest_setting.get("table_ddl_url")
    params = rest_setting.get("params")
    table_schemas = test_suite_config.get("table_schemas")
    for table_schema in table_schemas:
        table_name = table_schema.get("name")
        drop_table_if_exist_rest(table_ddl_url, table_name)

    for table_schema in table_schemas:
        create_table_rest(table_ddl_url, table_schema)
    return


def create_table_pyclient(client, table_schema):
    table_type = table_schema.get("type")
    table_name = table_schema.get("name")
    if table_type == "view":
        sql_2_run = table_schema.get("sql_2_run")
        logging.debug(f"create_table_pyclient: sql_2_run = {sql_2_run}")
        client.execute(sql_2_run)
        logging.debug(f"create_table_pyclient: done")


def drop_table_if_exist_pylient(client, table_schema):
    table_type = table_schema.get("type")
    table_name = table_schema.get("name")
    if table_type == "view":
        sql_2_run = f"drop view if exists {table_name}"
        logging.debug(f"drop_table_if_exist_pyclient: sql_2_run = {sql_2_run}")
        client.execute(sql_2_run)
        logging.debug(f"drop_table_if_exist_pyclient: view {table_name} droped")


def env_setup(
    client, rest_setting, test_suite_config, env_compose_file, proton_ci_mode
):
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
        logging.info(f"env_setup: docker compose up...")
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
            10
        )  # health check rest is not accurate, wait after docker compsoe up under github mode, remove later when it's fixed.

    table_ddl_url = rest_setting.get("table_ddl_url")
    params = rest_setting.get("params")
    table_schemas = test_suite_config.get("table_schemas")
    for table_schema in table_schemas:
        table_name = table_schema.get("name")
        table_type = table_schema.get("type")
        if table_type == "table":
            drop_table_if_exist_rest(table_ddl_url, table_name)


    for table_schema in table_schemas:
        table_type = table_schema.get("type")
        if table_type == "table":
            create_table_rest(table_ddl_url, table_schema)
        elif table_type == "view":
            create_table_pyclient(client, table_schema)

    setup = test_suite_config.get("setup")
    logging.debug(f"env_setup: setup = {setup}")
    if setup != None:
        setup_inputs = setup.get("inputs")
        if setup_inputs != None:
            setup_input_res = input_walk_through_rest(
                rest_setting, setup_inputs, table_schemas
            )

    return


# @pytest.fixture(scope="module")
def rockets_run(test_context):
    #todo: split tests.json to test_suite_config.json and tests.json
    logging.info("rockets_run starts......")
    docker_compose_file = test_context.get("docker_compose_file")
    config = test_context.get("config")
    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    q_exec_client = test_context.get("query_exe_client")
    query_conn = test_context.get("query_exe_parent_conn")
    q_exec_client_conn = test_context.get("query_exe_child_conn")
    query_exe_queue = test_context.get("query_exe_queue")
    query_results_queue = test_context.get("query_results_queue")
    q_exec_client.start()  # start the query execute process
    logging.debug(f"q_exec_client: {q_exec_client} started.")
    test_suite = test_context.get("test_suite")
    test_suite_config = test_suite.get("test_suite_config")
    table_ddl_url = rest_setting.get("table_ddl_url")
    table_schemas = test_suite_config.get("table_schemas")
    tests = test_suite.get("tests")
    tests_2_run = test_suite_config.get("tests_2_run")

    test_run_list = []
    test_run_id_list = []
    proton_ci_mode = os.getenv("PROTON_CI_MODE", "Github")
    #proton_ci_mode = "local" # for debug use.

    if tests_2_run == None:  # if tests_2_run is not set, run all tests.
        test_run_list = tests
    else:  # if tests_2_run is set in test_suite_config, run the id list.
        logging.debug(f"rockets_run: tests_2_run is configured as {tests_2_run}")
        ids_2_run = tests_2_run.get("ids_2_run")
        tags_2_run = tests_2_run.get("tags_2_run")
        tags_2_skip = tests_2_run.get("tags_2_skip")

        # if tags_2_run is defined, follow tags_2_run no matter ids_2_run setting
        if tags_2_run != None and len(tags_2_run) != 0:
            for test in tests:
                is_run = 0
                test_tags = test.get("tags")
                if test_tags != None and len(test_tags) != 0:
                    for tag in test_tags:
                        if tag in tags_2_run:
                            is_run += 1
                    if is_run:
                        test_run_list.append(test)
        elif ids_2_run != None and len(ids_2_run) != 0:
            if ids_2_run[0] == "all":
                test_run_list = tests
            else:
                for test in tests:
                    if test.get("id") in ids_2_run:
                        test_run_list.append(test)
        # skip tags set
        if tags_2_skip != None and len(tags_2_skip) != 0:
            test_candidates = test_run_list
            test_run_list = []
            i = 0
            while i < len(test_candidates):
                is_skip = 0
                test = test_candidates[i]
                test_name = test.get("name")
                test_tags = test.get("tags")
                test_id = test.get("id")
                for tag in test_tags:
                    if tag in tags_2_skip:
                        is_skip += 1
                if is_skip == 0:
                    test_run_list.append(test)
                i += 1

        for test in test_run_list:
            test_run_id_list.append(test.get("id"))

        assert len(test_run_list) != 0

        logging.debug(
            f"rockets_run: tests_run_id_list = {test_run_id_list}, {len(tests)} cases in total, {len(test_run_list)} cases to run in total"
        )

    logging.info("rockets_run env_etup done")

    test_id_run = 0
    test_sets = []
    try:
        client = Client(host=proton_server, port=proton_server_native_port)
        env_setup(
            client, rest_setting, test_suite_config, docker_compose_file, proton_ci_mode
        )
        logging.info("rockets_run env_etup done")

        test_id_run = 0
        test_sets = []
        # client = Client(host=proton_server, port=proton_server_native_port)
        while test_id_run < len(test_run_list):
            test_case = test_run_list[test_id_run]
            statements_results = []
            inputs_record = []
            test_id = test_case.get("id")
            test_name = test_case.get("name")
            steps = test_case.get("steps")
            # logging.debug(f"rockets_run: test_id = {test_id}, test_case = {test_case}, steps = {steps}")
            expected_results = test_case.get("expected_results")
            step_id = 0
            auto_terminate_queries = []
            # scan steps to find out tables used in inputs and truncate all the tables
            tables = []
            for step in steps:
                if "inputs" in step:
                    inputs = step.get("inputs")
                    for (
                        input
                    ) in inputs:  # clean table data before each inputs walk through
                        logging.debug(f"rockets_run: input in inputs = {input}")
                        table = input.get("table_name")
                        logging.debug(
                            f"rockets_run: table of input in inputs = {table}"
                        )
                        res = client.execute(f"drop table {table}")
                        tables.append(table)
                        logging.debug(
                            f"rockets_run: drop table {table} res = {res}"
                        )
                        
                        for table_schema in table_schemas:
                            name = table_schema.get("name")
                            if name == table and table_exist(table_ddl_url, table):
                                logging.debug(f"rockets_run, drop table and re-create once case starts, table_ddl_url = {table_ddl_url}, table_schema = {table_schema}") 
                                while table_exist(table_ddl_url, table): 
                                    logging.debug(f"{name} not dropped succesfully yet, wait ...")
                                    time.sleep(0.2)
                                logging.debug(f"rockets_run: drop table and re-create once case starts, table {table} is dropped")
                                res = create_table_rest(table_ddl_url, table_schema)
                if len(tables) > 0:
                    logging.debug(f"tables: {tables} are dropted and recreated.")
               
            
            for step in steps:
                statements_id = 0
                inputs_id = 0

                if "statements" in step:
                    step_statements = step.get("statements")
                    query_walk_through_res = query_walk_through(
                        step_statements, query_conn
                    )
                    statement_result_from_query_execute = query_walk_through_res
                    logging.debug(
                        f"rockets_run: query_walk_through_res = {query_walk_through_res}"
                    )

                    if (
                        statement_result_from_query_execute != None
                        and len(statement_result_from_query_execute) > 0
                    ):
                        for element in statement_result_from_query_execute:
                            statements_results.append(element)

                    logging.info(
                        f"rockets_run: {test_id_run}, test_id = {test_id}, step{step_id}.statements{statements_id}, done..."
                    )

                    statements_id += 1
                elif "inputs" in step:
                    inputs = step.get("inputs")
                    logging.info(
                        f"rockets_run: {test_id_run}, test_id = {test_id} inputs = {inputs}"
                    )

                    inputs_record = input_walk_through_rest(
                        rest_setting, inputs, table_schemas
                    )  # inputs walk through rest_client
                    logging.info(
                        f"rockets_run: {test_id_run}, test_id = {test_id} input_walk_through done"
                    )
                    # time.sleep(0.5) #wait for the data inputs done.
                step_id += 1

            query_conn.send("test_steps_done")


            message_recv = (
                query_conn.recv()
            )  # wait the query_execute to send "case_result_done" to indicate all the statements in pipe are consumed.

            logging.debug(
                f"rockets_run: mssage_recv from query_exe_queue = {message_recv}"
            )
            assert message_recv == "case_result_done"

            while (
                not query_results_queue.empty()
            ):  # collect all the query_results from queue after "case_result_done" received
                time.sleep(0.2)
                message_recv = query_results_queue.get()
                logging.debug(f"rockets_run: message_recv of query_results_queue.get() = {message_recv}")
                query_results = json.loads(message_recv)
                statements_results.append(query_results)

            test_sets.append(
                {
                    "test_id_run": test_id_run,
                    "test_id": test_id,
                    "test_name": test_name,
                    "steps": steps,
                    "expected_results": expected_results,
                    "statements_results": statements_results,
                }
            )
            test_id_run += 1

    except (BaseException) as error:
        logging.info("exception:", error)
    finally:
        TESTS_QUERY_RESULTS = test_sets
        query_conn.send("tear_down")
        message_recv = query_conn.recv()
        query_results_queue.close()
        query_conn.close()
        q_exec_client_conn.close()
        q_exec_client.terminate()
        q_exec_client.join()
        client.disconnect()
        return test_sets


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
        logging.info("no logging.conf exists under ../helper, no logging.")

    logging.info("rockets_main starts......")

    argv = sys.argv[1:]  # get -d to specify the test_sutie path
    try:
        opts, args = getopt.getopt(argv, "d:")
    except:
        logging.info("Error")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-d"]:
            test_suite_path = arg
    if test_suite_path == None:
        logging.info("No test suite directory specificed by -d, exit.")
        sys.exit(0)
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
        logging.info("No tests.json exists under test suite folder.")
