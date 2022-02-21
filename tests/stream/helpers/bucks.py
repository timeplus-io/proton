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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import rockets
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

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

# alive = mp.Value('b', False)
# todo: refactoring, Class Test and abstraction of test run logic in Rockets and reuse in performance test scripts.


def env_var_get():
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
    logger.debug(
        f"kill_query: datetime.now = {datetime.datetime.now()}, kill_sql = {kill_sql}."
    )
    kill_res = proton_client.execute(kill_sql)
    logger.debug(
        f"kill_query: kill_sql = {kill_sql} cmd executed, kill_res = {kill_res}"
    )
    print(f"kill_query: kill_sql = {kill_sql} cmd executed, kill_res = {kill_res}")
    while len(kill_res):
        time.sleep(0.2)
        kill_res = proton_client.execute(kill_sql)
        logger.debug(f"kill_query: kill_res = {kill_res}")


def row_reader(csv_file_path):
    print(f"row_reader: csv_file_path = {csv_file_path}")
    with open(csv_file_path) as csv_file:
        print(f"row_reader: csv_file = {csv_file}")
        for line in csv.reader(csv_file):
            yield line


def input_from_csv_by_row_reader(
    proton_server,
    proton_server_native_port,
    table_schema,
    csv_file_path,
    interval=0,
    data_sets_play_mode="repeat",
    rows_in_one_batch=3,
):
    print(f"input_from_csv: csv_file_path = {csv_file_path}")
    client = Client(host=proton_server, port=proton_server_native_port)
    columns = table_schema.get("columns")
    table_name = table_schema.get("name")
    table_columns = ""
    # for element in columns:
    #    table_columns = table_columns + element.get("name") + ","
    # table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
    if os.path.exists(csv_file_path):
        batch_str = ""
        batch_count = 0
        i = 0
        input_sql = ""
        for row in row_reader(csv_file_path):
            # print("input_walk_through: row:", row)
            row_str = " "
            if i == 0:
                for field in row:
                    table_columns = table_columns + field + ","
                table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
                i += 1
            else:
                for field in row:
                    # print("input_walk_through: field:", field)
                    if isinstance(field, str):
                        field.replace('"', '//"')  # proton does
                    row_str = (
                        row_str + "'" + str(field) + "'" + ","
                    )  # python client does not support "", so put ' here
                row_str = "(" + row_str[: len(row_str) - 1] + ")"

                input_sql = (
                    f"insert into {table_name} {table_columns_str} values {row_str}"
                )
                input_result = client.execute(input_sql)
                time.sleep(interval)
            print(f"input_from_csv: input_sql = {input_sql}")
            #
    else:
        raise Exception("csv file specificed does not exist")


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
        rockets.drop_table_if_exist_rest(table_ddl_url, table_name)

    for table_schema in table_schemas:
        rockets.create_table_rest(table_ddl_url, table_schema)
    return


def systest_context(config_file=None, tests_file=None):
    config = rockets.rockets_env_var_get()
    if config == None:
        with open(config_file) as f:
            config = json.load(f)
        # logger.debug(f"rockets_context: config reading from config files: {config}")

    if config == None:
        raise Exception("No config env vars nor config file")

    with open(tests_file) as f:
        test_suite = json.load(f)
        # logger.debug(f"rockets_systest_context: test_suite = {test_suite}")

    rockets_context = {
        "config": config,
        "test_suite": test_suite,
    }
    ROCKETS_CONTEXT = rockets_context
    return rockets_context


def create_table_from_column_list(table_ddl_url, table_name, column_list):
    # create table based on a given column_list like: [('id', 'String'), ('location', 'String'), ('value', 'Float32'), ('json', 'String'), ('timestamp', 'DateTime64(3)'), ('_tp_time', 'DateTime64(3)'), ('_tp_index_time', 'DateTime64(3)')])
    # print(f"create_table_from_column_list: table_ddl_url = {table_ddl_url}, table_name = {table_name}, column_list = {column_list}")
    table_schema_columns = []
    table_column_headers = []
    for column in column_list:
        table_schema_column = {}
        if isinstance(column, tuple):
            if "_tp" in column[0]:
                table_schema_column["name"] = "_" + column[0]
                table_schema_column["type"] = column[1]
                table_column_headers.append(table_schema_column["name"])
                table_schema_columns.append(table_schema_column)
            else:
                table_schema_column["name"] = column[0]
                table_schema_column["type"] = column[1]
                table_column_headers.append(table_schema_column["name"])
                table_schema_columns.append(table_schema_column)

        else:
            pass

    table_schema = {"name": table_name, "columns": table_schema_columns}
    print(f"create_table_from_column_list: table_schema = {table_schema}")
    res = create_table_rest(table_ddl_url, table_schema)
    print(f"create_table_from_column_list: res of create_table_rest = {res}")
    return {"res": res, "table_column_headers": table_column_headers}


def create_table_schema_from_query_result_column(table_name, column_list):
    # create table based on a given column_list like: [('id', 'String'), ('location', 'String'), ('value', 'Float32'), ('json', 'String'), ('timestamp', 'DateTime64(3)'), ('_tp_time', 'DateTime64(3)'), ('_tp_index_time', 'DateTime64(3)')])
    # print(f"create_table_from_column_list: table_ddl_url = {table_ddl_url}, table_name = {table_name}, column_list = {column_list}")
    table_schema_columns = []
    table_column_headers = []
    for column in column_list:
        table_schema_column = {}
        if isinstance(column, tuple):
            if "_tp" in column[0]:
                table_schema_column["name"] = "_" + column[0]
                table_schema_column["type"] = column[1]
                table_column_headers.append(table_schema_column["name"])
                table_schema_columns.append(table_schema_column)
            else:
                table_schema_column["name"] = column[0]
                table_schema_column["type"] = column[1]
                table_column_headers.append(table_schema_column["name"])
                table_schema_columns.append(table_schema_column)

        else:
            pass

    table_schema = {"name": table_name, "columns": table_schema_columns}

    return {"table_schema": table_schema, "table_column_headers": table_column_headers}


def query_results_2_list(query_results):
    print()  # transfer query_results into a list for input_sql_from_list(), and evantually query_results will be written into a table


def query_run_py(
    query_agent_id,
    statement_2_run,
    settings,
    query_done_semaphore,
    query_results_queue=None,
    config=None,
    pyclient=None,
):

    logger = mp.log_to_stderr()
    logger.setLevel(logging.DEBUG)

    if pyclient == None:
        proton_server = config.get("proton_server")
        proton_server_native_port = config.get("proton_server_native_port")
        settings = {"max_block_size": 100000}
        pyclient = Client(
            host=proton_server, port=proton_server_native_port
        )  # create python client
        CLEAN_CLIENT = True
    if config != None:
        rest_setting = config.get("rest_setting")
        table_ddl_url = rest_setting.get("table_ddl_url")
    query = statement_2_run.get("query")
    query_id = str(statement_2_run.get("query_id"))
    query_sub_id = str(statement_2_run.get("query_sub_id"))
    query_type = statement_2_run.get("query_type")
    run_mode = statement_2_run.get("run_mode")
    result_keep = statement_2_run.get("result_keep")

    query_record_table = statement_2_run.get("query_record_table")
    query_result_table = statement_2_run.get("query_result_table")

    query_start_time_str = str(datetime.datetime.now())
    query_end_time_str = str(datetime.datetime.now())
    element_json_str = ""
    query_result_str = ""
    query_result_column_types = []
    query_result_list = []

    query_record_file_name = (
        query_agent_id
        + "_"
        + "query_result_list"
        + str(datetime.datetime.now())
        + ".csv"
    )
    with open(query_record_file_name, "w") as f:
        writer = csv.writer(f)

        # logger.debug(f"query_run_py: query_id = {query_id}, query = {query} to be execute.........")
        try:
            query_result_iter = pyclient.execute_iter(
                query, with_column_types=True, query_id=query_sub_id, settings=settings
            )
            i = 0
            for element in query_result_iter:
                # logger.debug(f"query_run_py: element in query_result_iter in query_id: {query_id} = {element}")
                if result_keep != None and result_keep == "False":
                    pass
                else:
                    if isinstance(element, list) or isinstance(element, tuple):
                        element = list(element)
                        element.append(str(datetime.datetime.now()))

                    if i == 0:
                        query_result_column_types = element
                        # logger.debug(f"query_run_py: query_result_colume_types in query_iter = {query_result_column_types}")
                    else:
                        # logger.debug(f"query_run_py: element before tuple_2_list = {element}")
                        element_list = tuple_2_list(element)
                        # logger.debug(f"query_run_py: element_list in query_result_iter in query_id: {query_id} = {element_list}")
                        writer.writerow(element)
                i += 1
            query_end_time_str = str(
                datetime.datetime.now()
            )  # record query_end_time, and transfer to str

            if run_mode == "process" or query_type == "stream":
                pyclient.disconnect()

        except (errors.ServerException) as error:
            logger.debug("query_run_py: running in exception......")
            if isinstance(error, errors.ServerException):
                if (
                    error.code == 394
                ):  # if the query is canceled '394' will be caught and compose the query_results and send to inputs_walk_through
                    # send the result
                    pass

                else:  # for other exception code, send the error_code as query_result back, some tests expect eception will use.
                    writer.writerow(["ServerException", error.code])
            else:
                writer.writerow(["Exception", error.code])

        if run_mode == "process" or query_type == "stream":
            pyclient.disconnect()

    # logger.debug(f"query_run_py: query_id = {query_id}, query={query}, query_results = {query_results}")
    print(f"query_run_py: ended.")
    return


def query_execute(config, child_conn, query_results_queue, alive):
    # logging.basicConfig(level=logger.debug, filename="rockets.log")
    # query_result_list = query_result_list
    logger = mp.log_to_stderr()
    logger.setLevel(logging.DEBUG)

    logger.debug(f"bucks: query_execute starts...")

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

            logger.debug(
                f"query_execute: tear_down = {tear_down}, query_run_count = {query_run_count}, wait for message from rockets_run......"
            )
            message_recv = child_conn.recv()
            logger.debug(f"query_execute: message_recv = {message_recv}")

            if message_recv == "tear_down":
                tear_down = True
                break

            elif message_recv == "test_steps_done":
                # logger.debug(
                #    f"query_execute: test_steps_done received @ {datetime.datetime.now()}, query_procs={query_procs}"
                # )
                retry = 1000  # if one case run out 600s, all the processes running the streaming queries will be killed, be careful about this timeout, if some case do need run longer time, tune this setting!
                num_of_procs_2_end = len(query_procs)

                if query_end_timer != None:
                    time.sleep(int(query_end_timer))

                i = 0
                while i < num_of_procs_2_end and retry > 0:
                    # for proc in query_procs:
                    for proc in query_procs:
                        process = proc.get("process")
                        query_id = proc.get("query_id")
                        query_sub_id = proc.get("query_sub_id")
                        exitcode = process.exitcode
                        terminate = proc.get("terminate")
                        query_end_timer = proc.get("query_end_timer")
                        logger.debug(
                            f"query_execute: query_procs pop, process = {process}, exitcode = {exitcode}"
                        )

                        if process.exitcode != None:
                            i += 1
                        # else:
                        elif terminate == "auto":
                            kill_query(client, query_sub_id)
                            logger.debug(
                                f"query_execute: kill_query query_id = {query_sub_id} is executed."
                            )

                        # logger.debug(
                        #    f"query_execute: query_procs = {query_procs} after trying to remove"
                        # )
                    time.sleep(3)

                    retry = retry - 1

                if retry == 0:  # terminate all the query process after retry timeout
                    for proc in query_procs:
                        process = proc.get("process")
                        exitcode = process.exitcode
                        if process.exitcode == None:
                            process.terminate()
                            query = proc.get("query")
                            logger.debug(
                                f"query_execute: process={process}, query={query} is terminated after retry timeout."
                            )
                query_procs = (
                    []
                )  # only when all the proccesses are terminated, query_procs list could be reset.

                message_2_send = "case_result_done"
                # query_exe_queue.put(message_2_send)
                child_conn.send(message_2_send)
                query_run_count -= 1

            else:
                statement_2_run = json.loads(json.dumps(message_recv))
                logger.debug(f"query_execute: statement_2_run = {statement_2_run}")
                query_id = str(statement_2_run.get("query_id"))
                query_client = statement_2_run.get("client")
                query_type = statement_2_run.get("query_type")
                workers = statement_2_run.get("workers")
                workers = int(workers) if workers != None else 1
                terminate = statement_2_run.get("terminate")
                if terminate == "auto":
                    auto_terminate_queries.append(statement_2_run)
                run_mode = statement_2_run.get("run_mode")

                query = statement_2_run.get("query")
                query_end_timer = statement_2_run.get("query_end_timer")
                query_start_time_str = str(datetime.datetime.now())
                query_end_time_str = str(datetime.datetime.now())

                for i in range(workers):
                    query_done_semaphore = mp.Value("b", False)
                    query_agent_id = "query_agent_" + str(
                        i
                    )  # the actual query_id for query execution and cancel
                    query_sub_id = query_id + "_" + str(i)
                    statement_2_run[
                        "query_sub_id"
                    ] = query_sub_id  # add sub_query_id into statement_2_run
                    query_run_args = (
                        query_agent_id,
                        statement_2_run,
                        settings,
                        query_done_semaphore,
                        query_results_queue,
                        config,
                    )
                    query_agent_id = "query_agent_" + str(i)
                    query_proc = mp.Process(target=query_run_py, args=query_run_args)

                    query_proc.start()
                    query_procs.append(
                        {
                            "process": query_proc,
                            "terminate": terminate,
                            "query_end_timer": query_end_timer,
                            "query_id": query_id,
                            "query_sub_id": query_sub_id,
                            "query": query,
                            "query_done": query_done_semaphore,
                        }
                    )

                    # logger.debug(
                    #    f"query_execute: start a proc for query = {query}, query_run_args = {query_run_args}, query_proc.pid = {query_proc.pid}"
                    # )

                message_2_send = f"query proc started for query_id = {query_id}"
                child_conn.send(message_2_send)
                query_run_count = query_run_count - 1
        except (BaseException, errors.ServerException) as error:
            logger.debug(f"query_execute: error = {error}")
            query_run_count = query_run_count - 1
    print(f"query_execute: tear down......")
    if query_run_count == 0:
        logger.debug(
            "Super, 1000 queries hit by a single test suite, we are in great time, by James @ Jan 10, 2022!"
        )
    if len(query_procs) != 0:
        print(f"query_execute: query_procs not empty = {query_procs} ")
        for proc in query_procs:
            process = proc.get("process")
            process.terminate()
            process.join()
    client.disconnect()
    # query_exe_queue.put("tear_down_done")
    child_conn.send("tear_down_done")
    logger.debug(f"query_execute: tear_down completed and end")


def query_walk_through(statements, query_conn=None):
    logger.debug(f"query_walk_through: start..., statements = {statements}.")
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

        logger.debug(f"query_walk_through: statement = {statement}.")
        if query_end_timer == None:
            query_end_timer = 0

        # query_exe_queue.put(statement)
        query_conn.send(statement)
        logger.debug(
            # f"query_walk_through: statement query_id = {query_id} was pushed into query_exe_queue."
            f"query_walk_through: statement query_id = {query_id} was send to query_execute."
        )
        message_recv = (
            query_conn.recv()
        )  # wait for the message from query_execute to indicate the query process is started.
        logger.debug(
            f"query_walk_through: message_recv = {message_recv} received after send statement to query_execute"
        )

        if isinstance(wait, dict):  # if wait for a specific query done
            print()  # todo: check the query_id and implement the logic to notify the query_execute that this query need to be done after the query to be wait done and implement the wait logic in query_execute_new
        elif str(wait).isdigit():  # if wait for x seconds and then execute the query
            time.sleep(wait)

        statement_id_run += 1
        # time.sleep(1) # wait the query_execute execute the stream command

    logger.debug(f"query_walk_through: end... stream_query_id = {stream_query_id}")
    return querys_results


def result_collect():
    print()  # collect all the data from files and write to database for ultra data analytics.


def clear_case_env(client, test, table_schemas, table_ddl_url):
    steps = test.get("steps")
    tables = []
    for step in steps:
        if "inputs" in step:
            inputs = step.get("inputs")
            for input in inputs:  # clean table data before each inputs walk through
                logger.debug(f"rockets_run: input in inputs = {input}")
                table = input.get("table_name")
                logger.debug(f"rockets_run: table of input in inputs = {table}")

                is_table_reset = find_table_reset_in_table_schemas(table, table_schemas)
                if is_table_reset != None and is_table_reset == False:
                    pass
                else:
                    res = client.execute(f"drop table {table}")
                    tables.append(table)
                    logger.debug(f"rockets_run: drop table {table} res = {res}")

                    for table_schema in table_schemas:
                        name = table_schema.get("name")
                        if name == table and table_exist(table_ddl_url, table):
                            logger.debug(
                                f"rockets_run, drop table and re-create once case starts, table_ddl_url = {table_ddl_url}, table_schema = {table_schema}"
                            )
                            while table_exist(table_ddl_url, table):
                                logger.debug(
                                    f"{name} not dropped succesfully yet, wait ..."
                                )
                                time.sleep(0.2)
                            logger.debug(
                                f"rockets_run: drop table and re-create once case starts, table {table} is dropped"
                            )
                            res = create_table_rest(table_ddl_url, table_schema)
        if len(tables) > 0:
            logger.debug(f"tables: {tables} are dropted and recreated.")


def data_prep_csv_2_list(
    csv_file_path,
    test_id,
    input_id,
    agent_id_pre_fix="agent_",
    copies=1,
    rows_2_play=10000,
    perf_event_time_start="2021-06-29 21:37:00",
    time_incre_interval=1,
    batch_size=1,
):

    logger.debug(f"data_prep_csv_2_list: copies = {copies}")

    data_set_seed = []  # list, read csv_file and put all lines in
    _list = []  # list created based on data_set seed, add row_id, perf_event_time

    _data_sets_list = (
        []
    )  # list of data_sets for all the input workers, based on _list, agent_id is added.
    _input_info_list = (
        []
    )  # list of input_info to link the unique perf_row_id to input_record and query_resutl and etc. for perf analytics.

    print(f"row_reader: csv_file_path = {csv_file_path}")
    perf_event_time = datetime.datetime.fromisoformat(perf_event_time_start)

    with open(csv_file_path) as csv_file:
        for line in csv.reader(csv_file):
            data_set_seed.append(line)
    # logger.debug(f"{sys._getframe().f_code.co_name}: data_set_seed = {data_set_seed}")
    data_set_seed_rows = len(data_set_seed)
    seed_play_times = int(rows_2_play / int(data_set_seed_rows)) + 1
    seed_play_mod_rows = rows_2_play % data_set_seed_rows
    logger.debug(
        f"{sys._getframe().f_code.co_name}: data_set_seed_rows = {data_set_seed_rows}, rows_2_play = {rows_2_play}, seed_play_times = {seed_play_times}"
    )

    # now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # perf_row_id = int(now_time)
    perf_batch_id = 0
    logger.debug(f"data_prep_2_list: perf_batch_id = {perf_batch_id} ")

    row_count = 0
    j = 0
    while j <= seed_play_times:  # prepare the data_set list based on the seed csv file.
        i = 0  # line counter, identify the header line
        copy_of_data_set_seed = [i[:] for i in data_set_seed]
        # print(f"copy_of_data_set_seed = {copy_of_data_set_seed}")
        for line in copy_of_data_set_seed:
            if j == 0:
                if (
                    i == 0
                ):  # only when the 1st time, the header line need to be appended to the list
                    line.append("perf_event_time")

                else:
                    line.append(str(perf_event_time))
                    row_count += 1
            else:
                if i == 0:
                    pass
                else:
                    line.append(str(perf_event_time))
                    row_count += 1
            if j == 0:
                _list.append(line)
            elif i != 0:
                _list.append(line)

            perf_event_time = perf_event_time + datetime.timedelta(
                seconds=time_incre_interval
            )
            # logger.debug(f"{sys._getframe().f_code.co_name}: line in csv_file after process: {line}")
            i += 1
            if row_count >= rows_2_play:
                break
        j += 1

    j = 0
    row_count = 0
    _input_info_row = []

    while j < copies:
        copy = [i[:] for i in _list]
        agent_id = agent_id_pre_fix + str(j)
        input_sub_id = str(input_id) + "_" + str(j)
        _data_sets_list.append(
            {
                "agent_id": agent_id,
                "test_id": test_id,
                "input_id": input_id,
                "input_sub_id": input_sub_id,
                "data_set": copy,
            }
        )
        j += 1

    for line in _list:
        line = []  # release mem of _list
    del _list
    for line in copy_of_data_set_seed:
        line = []
    del copy_of_data_set_seed  # release mem of list

    return _data_sets_list


def input_sql_from_list(table_name, data_set_list_with_header, batch_size=1):
    logger.debug(f"input_sql_from_list: batch_size = {batch_size}")
    table_columns = ""
    input_sql_list = []
    i = 0  # row_index to identify the fist row as header
    j = 0  # as batch counter
    batch_str = ""
    for row in data_set_list_with_header:
        # print("input_walk_through: row:", row)
        row_str = ""
        if i == 0:
            for field in row:
                table_columns = table_columns + field + ","
            table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
            i += 1
        else:
            for field in row:
                # print("input_walk_through: field:", field)
                if isinstance(field, str):
                    # field.replace('"', '\\"')  # proton does
                    field = field.replace("'", '"')
                    row_str = (
                        row_str + "'" + field + "'" + ","
                    )  # python client does not support "", so put ' here
                elif isinstance(field, list):
                    row_str = row_str + "'" + "["
                    for item in field:
                        row_str = row_str + '"' + str(item) + '"' + ","
                    row_str = row_str[: len(row_str) - 1] + "]" + "'" + ","
                else:
                    row_str = (
                        row_str + "'" + str(field) + "'" + ","
                    )  # python client does not support "", so put ' here
            row_str = "(" + row_str[: len(row_str) - 1] + ")"
            batch_str = batch_str + row_str + ","

            if j >= batch_size - 1:

                batch_str = batch_str[: len(batch_str) - 1]
                batch_str = batch_str.replace("'", "'")

                input_sql = (
                    f"insert into {table_name} {table_columns_str} values {batch_str}"
                )
                input_sql_list.append(input_sql)
                batch_str = ""
                j = 0
            else:
                j += 1

    if len(batch_str) != 0 and batch_str[len(batch_str) - 1] == ",":
        batch_str = batch_str[: len(batch_str) - 1]
        batch_str = batch_str.replace("'", "'")
        input_sql = f"insert into {table_name} {table_columns_str} values {batch_str}"
        input_sql_list.append(input_sql)
    # logger.debug(f"input_sql_from_list: input_sql_list = {input_sql_list}")
    return input_sql_list


"""
def input_sql_from_list(table_name, data_set_list_with_header, batch_size=1):
    logger.debug(f"input_sql_from_list: batch_size = {batch_size}")
    table_columns = ""
    input_sql_list = []
    i = 0  # row_index to identify the fist row as header
    j = 0  # as batch counter
    batch_str = ""
    for row in data_set_list_with_header:
        # print("input_walk_through: row:", row)
        row_str = ""
        if i == 0:
            for field in row:
                table_columns = table_columns + field + ","
            table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
            i += 1
        else:
            for field in row:
                # print("input_walk_through: field:", field)
                if isinstance(field, str):
                    field.replace('"', '//"')  # proton does
                row_str = (
                    row_str + "'" + str(field) + "'" + ","
                )  # python client does not support "", so put ' here
            row_str = "(" + row_str[: len(row_str) - 1] + ")"
            batch_str = batch_str + row_str + ","

            if j >= batch_size - 1:

                batch_str = batch_str[: len(batch_str) - 1]
                input_sql = (
                    f"insert into {table_name} {table_columns_str} values {batch_str}"
                )
                input_sql_list.append(input_sql)
                batch_str = ""
                j = 0
            else:
                j += 1

    if len(batch_str) != 0 and batch_str[len(batch_str) - 1] == ",":
        batch_str = batch_str[: len(batch_str) - 1]
        input_sql = f"insert into {table_name} {table_columns_str} values {batch_str}"
        input_sql_list.append(input_sql)
    # logger.debug(f"input_sql_from_list: input_sql_list = {input_sql_list}")
    return input_sql_list
"""


def input_client(
    config,
    source,
    input_sub_id,
    data_set,
    agent_id,
    input_record_table,
    input_tear_down,
    input_done,
    interval=0.5,
    data_sets_play_mode="sequence",
    batch_size=1,
    loop_times=0,
):
    # print(f"input_from_csv: csv_file_path = {csv_file_path}")

    logger = mp.get_logger()

    # formatter = logging.Formatter(
    #    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
    # )
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

    # logger.setLevel(logging.DEBUG)

    test_id = source.get("test_id")
    input_id = source.get("input_id")
    table_name = source.get("table_name")
    result_keep = source.get("result_keep")
    loop_times = int(source.get("loop_times"))

    print(
        f"input_client: worker for agend_id = {agent_id} started... input_tear_down.value = {input_tear_down.value}"
    )
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    client = Client(
        host=proton_server, port=proton_server_native_port
    )  # create python client

    # metric_create_batch_time = [] #for caculating time spent for creating a batch, for a batch of 1000 row, 20ms will be spent baed on the rough test
    if data_sets_play_mode == "sequence":
        loop_count = 0
        loop_limit = 10 if loop_times < 0 else loop_times
        while loop_count < loop_limit:
            table_columns = ""
            input_sql_list = []
            i = 0  # row_index to identify the fist row as header
            j = 0  # as batch counter
            batch_str = ""
            for row in data_set:
                # print("input_walk_through: row:", row)
                row_str = ""
                if i == 0:
                    for field in row:
                        table_columns = table_columns + field + ","
                    table_columns_str = (
                        "("
                        + table_columns
                        + "_perf_row_id"
                        + ","
                        + "_perf_ingest_time"
                        + ")"
                    )
                    # table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
                    i += 1
                else:
                    # if j == 0: metric_create_batch_time.append(f"start create one batch......, batch_size = {batch_size}, now = {str(datetime.datetime.now())}")
                    for field in row:
                        # print("input_walk_through: field:", field)
                        if isinstance(field, str):
                            field.replace('"', '//"')  # proton does
                        row_str = (
                            row_str + "'" + str(field) + "'" + ","
                        )  # python client does not support "", so put ' here
                    _perf_row_id = str(uuid.uuid1())
                    _perf_ingest_time = str(datetime.datetime.now())
                    row_str = (
                        "("
                        + row_str
                        + "'"
                        + _perf_row_id
                        + "'"
                        + ","
                        + "'"
                        + _perf_ingest_time
                        + "'"
                        + ")"
                    )
                    # row_str = "(" + row_str[: len(row_str) - 1] + ")"
                    batch_str = batch_str + row_str + ","

                    if j >= batch_size - 1:

                        batch_str = batch_str[: len(batch_str) - 1]
                        input_sql = f"insert into {table_name} {table_columns_str} values {batch_str}"
                        logger.debug(f"input_client: input_sql = {input_sql}")
                        # metric_create_batch_time.append(f"complete create one batch......, batch_size = {batch_size}, now = {str(datetime.datetime.now())}")
                        client.execute(input_sql)

                        # input_sql_list.append(input_sql)
                        batch_str = ""
                        j = 0

                    else:
                        j += 1

            if len(batch_str) != 0 and batch_str[len(batch_str) - 1] == ",":
                batch_str = batch_str[: len(batch_str) - 1]
                input_sql = (
                    f"insert into {table_name} {table_columns_str} values {batch_str}"
                )
                # print(f"input_client: input_sql = {input_sql}")
                # metric_create_batch_time.append(f"complete create one batch......, batch_size = {batch_size}, now = {str(datetime.datetime.now())}")
                client.execute(input_sql)

            # print(metric_create_batch_time)

            if loop_times > 0:
                loop_count += 1  # if loop_times < 0, run infinitely

        input_done.value = True  # set input_done mp.Value to True to indicate all the inputs are executed.

        print(f"input_client: waiting for input_tear_down......")
    elif data_sets_play_mode == "random":
        print()  # play randomly
    client.disconnect()


def input_walk_through(
    config,
    test_id,
    inputs,
    table_schemas,
    wait_before_inputs=1,
    sleep_after_inputs=1.5,  # stable set wait_before_inputs=1, sleep_after_inputs=1.5
    alive=None,
):

    # logger.debug(f"input_walk_through: config = {config}, inputs = {inputs}, table_schemas = {table_schemas}, wait_before_inputs = {wait_before_inputs}, sleep_after_inputs = {sleep_after_inputs}, alive = {alive}")

    wait_before_inputs = wait_before_inputs  # the seconds sleep before inputs starts to ensure the query is run on proton.
    sleep_after_inputs = sleep_after_inputs  # the seconds sleep after evary inputs of a case to ensure the stream query result was emmited by proton and received by the query execute
    time.sleep(wait_before_inputs)
    inputs_record = []
    data_set_abspath = None

    for source in inputs:
        table_name = source.get("table_name")
        input_id = source.get("input_id")
        table_schema = find_schema(table_name, table_schemas)
        workers = source.get("workers")
        data_source = source.get("data_source")
        data_set_path = source.get("data_set_path")
        input_record_table = source.get("input_record_table")

        if data_set_path != None:
            data_set_abspath = (
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                + "/"
                + "performance/configs/data/data_sets"
                + "/"
                + data_set_path
            )

        data_set_file = source.get("data_set_file")
        logger.debug(f"input_walk_through: data_set_file = {data_set_file}")
        if data_set_file != None:
            data_set_file_abspath = data_set_abspath + "/" + data_set_file

        assert os.path.exists(data_set_file_abspath)
        logger.debug(
            f"input_walk_through: data_set_file_abspath = {data_set_file_abspath} is found."
        )

        ingest_interval = source.get("ingest_interval")
        time_incre_interval = source.get("time_incre_interval")
        data_set_play_mode = source.get("data_set_play_mode")
        rows_2_play = source.get("rows_2_play")
        if rows_2_play != None:
            rows_2_play = int(rows_2_play)
        batch_size = source.get("batch_size")
        if batch_size != None:
            batch_size = int(batch_size)

        logger.debug(f"input_walk_through: workers = {workers}")

        data_sets_for_workers = data_prep_csv_2_list(
            data_set_file_abspath,
            test_id,
            input_id,
            copies=workers,
            rows_2_play=rows_2_play,
            perf_event_time_start="2021-06-29 21:37:00",
            time_incre_interval=ingest_interval,
            batch_size=batch_size,
        )

        # logger.debug(f"{sys._getframe().f_code.co_name}: data_sets_for_workers = {data_sets_for_workers}, input_info_data_set = {input_info_data_set}")

        proc_workers = []
        for data_set_dict in data_sets_for_workers:
            input_sub_id = data_set_dict.get("input_sub_id")
            data_set = data_set_dict.get("data_set")
            agent_id = data_set_dict.get("agent_id")
            input_done = mp.Value("b", False)
            input_tear_down = mp.Value("b", False)
            # logger.debug(f"{sys._getframe().f_code.co_name}: agent_id = {agent_id}, data_set = {data_set}")
            # logger.debug(f"worker for agend_id = {agent_id} to be started..., alive = {alive}, alive.value = {alive.value}")

            proc = mp.Process(
                target=input_client,
                args=(
                    config,
                    source,
                    input_sub_id,
                    data_set,
                    agent_id,
                    input_record_table,
                    input_tear_down,
                    input_done,
                    ingest_interval,
                    data_set_play_mode,
                    batch_size,
                ),
            )
            proc.start()
            proc_workers.append(
                {
                    "agent_id": agent_id,
                    "proc": proc,
                    "input_done": input_done,
                    "input_tear_down": input_tear_down,
                }
            )

    time.sleep(sleep_after_inputs)
    return proc_workers


def test_suite_run(test_context, proc_target_func=query_execute):
    # todo: split tests.json to test_suite_config.json and tests.json
    alive = mp.Value("b", False)
    logger.info("rockets_run starts......")
    docker_compose_file = test_context.get("docker_compose_file")
    config = test_context.get("config")
    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_port = config.get("proton_server_native_port")
    alive.value = True

    (
        query_conn,
        query_exe_child_conn,
    ) = (
        mp.Pipe()
    )  # create the pipe for inter-process conn of rockets_run and query_execute, control path

    query_results_queue = (
        mp.Queue()
    )  # data path queue for query results, query_execute and threads created by query_execute process pushed query results into this queue.

    query_exe_client = mp.Process(
        target=proc_target_func,
        args=(config, query_exe_child_conn, query_results_queue, alive),
    )  # Create query_exe_client process

    alive.value = True
    query_exe_client.start()  # start the query execute process
    logger.debug(f"q_exec_client: {query_exe_client} started.")
    test_suite = test_context.get("test_suite")
    test_suite_config = test_suite.get("test_suite_config")
    table_ddl_url = rest_setting.get("table_ddl_url")
    table_schemas = test_suite_config.get("table_schemas")
    tests = test_suite.get("tests")
    tests_2_run = test_suite_config.get("tests_2_run")

    test_run_list = []
    test_run_id_list = []

    proton_ci_mode = test_suite_config.get("proton_ci_mode")
    if proton_ci_mode == None:
        proton_ci_mode = os.getenv(
            "PROTON_CI_MODE", "Github"
        )  # if proton_ci_mode is set in test_suite_config, it will overwrite the env var setting.

    if tests_2_run == None:  # if tests_2_run is not set, run all tests.
        test_run_list = tests
    else:  # if tests_2_run is set in test_suite_config, run the id list.
        logger.debug(f"rockets_run: tests_2_run is configured as {tests_2_run}")
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

        logger.debug(
            f"rockets_run: tests_run_id_list = {test_run_id_list}, {len(tests)} cases in total, {len(test_run_list)} cases to run in total"
        )

    logger.info("rockets_run env_etup done")

    test_id_run = 0
    test_sets = []
    input_procs = []
    all_input_procs = []
    input_info_data_set = []

    try:
        client = Client(host=proton_server, port=proton_server_native_port)
        env_setup(
            client, rest_setting, test_suite_config, docker_compose_file, proton_ci_mode
        )
        logger.info("rockets_run env_etup done")

        test_id_run = 0
        test_sets = []
        # client = Client(host=proton_server, port=proton_server_native_port)
        while test_id_run < len(test_run_list):
            test_case = test_run_list[test_id_run]
            statements_results = []
            inputs_record = []
            test_id = test_case.get("id")
            test_name = test_case.get("name")
            input_info_table = test_case.get("input_info_table")
            steps = test_case.get("steps")
            # logger.debug(f"rockets_run: test_id = {test_id}, test_case = {test_case}, steps = {steps}")
            expected_results = test_case.get("expected_results")
            step_id = 0
            auto_terminate_queries = []
            # scan steps to find out tables used in inputs and truncate all the tables

            res_clear_case_env = reset_tables_of_test_inputs(
                client, table_ddl_url, table_schemas, test
            )

            for step in steps:
                statements_id = 0
                inputs_id = 0

                if "statements" in step:
                    step_statements = step.get("statements")
                    query_walk_through_res = query_walk_through(
                        step_statements, query_conn
                    )
                    statement_result_from_query_execute = query_walk_through_res
                    logger.debug(
                        f"rockets_run: query_walk_through_res = {query_walk_through_res}"
                    )

                    if (
                        statement_result_from_query_execute != None
                        and len(statement_result_from_query_execute) > 0
                    ):
                        for element in statement_result_from_query_execute:
                            statements_results.append(element)

                    logger.debug(
                        f"rockets_run: {test_id_run}, test_id = {test_id}, step{step_id}.statements{statements_id}, done..."
                    )

                    statements_id += 1
                elif "inputs" in step:
                    inputs = step.get("inputs")
                    logger.info(
                        f"rockets_run: {test_id_run}, test_id = {test_id} inputs = {inputs}"
                    )
                    logger.debug(f"test_suite_run: alive.value = {alive.value}")

                    input_procs = input_walk_through(
                        config, test_id, inputs, table_schemas, alive=alive
                    )  # inputs walk through py_client

                    all_inputs_done = 0
                    while all_inputs_done == 0:  # todo: optimize algorithm
                        done_flags = []
                        for input_proc in input_procs:
                            all_input_procs.append(input_proc)
                            agent_id = input_proc.get("agent_id")
                            proc = input_proc.get("proc")

                            proc_input_done = input_proc.get("input_done")
                            proc_input_done_flag = 1 if proc_input_done.value else 0
                            done_flags.append(proc_input_done_flag)
                        all_inputs_done = 1
                        for flag in done_flags:
                            all_inputs_done = all_inputs_done * flag
                        time.sleep(1)

                    logger.debug(
                        f"rockets_run: {test_id_run}, test_id = {test_id} input_walk_through done, input_record will be written after results_down to avoid impact to perf test"
                    )
                    # time.sleep(0.5) #wait for the data inputs done.

                step_id += 1

            query_conn.send("test_steps_done")

            message_recv = (
                query_conn.recv()
            )  # wait the query_execute to send "case_result_done" to indicate all the statements in pipe are consumed.

            logger.debug(
                f"rockets_run: mssage_recv from query_execute = {message_recv}"
            )
            assert message_recv == "case_result_done"

            for (
                input_proc
            ) in input_procs:  # tear down all input processes to write input_record
                input_tear_down = input_proc.get("input_tear_down")
                input_tear_down.value = True

            test_id_run += 1

        test_sets = result_collect()
    except (BaseException) as error:
        logger.info("exception:", error)
    finally:
        TESTS_QUERY_RESULTS = test_sets
        query_conn.send("tear_down")
        message_recv = query_conn.recv()
        logger.debug(
            f"test_suite_run: message_recv got after tear down sent = {message_recv}"
        )
        query_results_queue.close()
        query_conn.close()
        # q_exec_client_conn.close()
        query_exe_child_conn.close()
        client.disconnect()
        alive.value = False
        # q_exec_client.terminate()
        # q_exec_client.join()
        logger.debug(f"test_suite_run: input processes tear down ......")
        for proc in all_input_procs:
            inpput_tear_down = proc.get("inoput_tear_down")
            input_done = proc.get("input_done")
            proc["proc"].join()
            del input_tear_down
            del input_done

        logger.debug(f"test_suite_run: query processes tear down ......")
        query_exe_client.join()

        return test_sets


def test_suite_context(config_file=None, tests_file=None, docker_compose_file=None):
    # global alive
    # logger.debug(f"test_context: proc_target_func = {proc_target_func}, config_file = {config_file}, tests_file = {tests_file}, docker_compose_file = {docker_compose_file}")
    config = env_var_get()
    if config == None:
        with open(config_file) as f:
            config = json.load(f)
        logger.debug(f"rockets_context: config reading from config files: {config}")

    if config == None:
        raise Exception("No config env vars nor config file")
    # proton_server = config.get("proton_server")
    # proton_server_native_port = config.get("proton_server_native_port")

    logger.debug(f"test_context: tests_file = {tests_file}")
    with open(tests_file) as f:
        test_suite = json.load(f)

    rockets_context = {
        "config": config,
        "test_suite": test_suite,
        # "query_exe_client": query_exe_client,
        "docker_compose_file": docker_compose_file,
        # "query_exe_parent_conn": query_exe_parent_conn,
        # "query_exe_child_conn": query_exe_child_conn,
        # "query_results_queue": query_results_queue,
    }

    ROCKETS_CONTEXT = rockets_context
    return rockets_context


if __name__ == "__main__":

    cur_file_path = os.path.dirname(os.path.abspath(__file__))
    cur_file_path_parent = os.path.dirname(cur_file_path)
    test_suite_path = None

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

    logger.setLevel(logging.INFO)

    logger.info("rockets_main starts......")

    argv = sys.argv[1:]  # get -d to specify the test_sutie path
    try:
        opts, args = getopt.getopt(argv, "d:")
    except:
        logger.info("Error")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-d"]:
            test_suite_path = arg
    if test_suite_path == None:
        logger.info("No test suite directory specificed by -d, exit.")
        sys.exit(0)
    config_file = f"{test_suite_path}/configs/config.json"
    tests_file = f"{test_suite_path}/tests.json"
    docker_compose_file = f"{test_suite_path}/configs/docker-compose.yaml"

    if os.path.exists(tests_file):
        test_context = test_suite_context(
            config_file, tests_file, docker_compose_file
        )  # need to have config env vars/config.json and test.json when run rockets.py as a test debug tooling.
        test_sets = test_suite_run(test_context, query_execute)
        # output the test_sets one by one
        logger.info("main: ouput test_sets......")
        # for test_set in test_sets:
        #    test_set_json = json.dumps(test_set)
        #    logger.info(f"main: test_set from rockets_run: {test_set_json} \n\n")
    else:
        logger.info("No tests.json exists under test suite folder.")
