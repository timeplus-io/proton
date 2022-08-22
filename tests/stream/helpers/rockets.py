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
#   7. clean test environment, drop stream, clean pipes and etc.
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

from cgi import test
import os, sys, json, getopt, subprocess, traceback
import logging, logging.config
import time
import datetime
import random
import requests
import uuid
import multiprocessing as mp
from clickhouse_driver import Client
from clickhouse_driver import errors
from requests.api import request
from helpers.utils import compose_up


cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

TABLE_CREATE_RECORDS = []
TABLE_DROP_RECORDS = []
VIEW_CREATE_RECORDS = []
QUERY_RUN_RECORDS = []


# alive = mp.Value('b', True)


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


def scan_tests_file_path(tests_file_path):
    test_suites_selected = []
    test_suite_names_selected = []
    all_test_suites_json = []
    test_suites_set_list = []
    test_suites_set_env = os.getenv("PROTON_TEST_SUITES", None)
    if test_suites_set_env != None:
        test_suites_set_list = test_suites_set_env.split(",")

    logger.info(
        f"tests_file_path = {tests_file_path}, test_suites_env = {test_suites_set_env}, test_suite_set_list = {test_suites_set_list}"
    )
    files = os.listdir(tests_file_path)
    logger.debug(f"files = {files}")
    for file_name in files:
        if file_name.endswith(".json"):
            file_abs_path = f"{tests_file_path}/{file_name}"
            logger.debug(f"file_abs_path = {file_abs_path}")
            with open(file_abs_path) as test_suite_file:
                test_suite = json.load(test_suite_file, strict=False)
                logger.debug(
                    f"test_suite_file = {test_suite_file}, was loaded successfully."
                )
                test_suite_name = test_suite.get("test_suite_name")
                test_suite_tag = test_suite.get("tag")
                if test_suite_name == None or test_suite_tag == "skip":
                    logger.debug(
                        f"test_suite_name is vacant or test_suite_tag == skip and ignore this json file"
                    )
                    pass
                else:
                    logger.debug(
                        f"check if test_sute_name = {test_suite_name} in test_suites_set_list = {test_suites_set_list}"
                    )
                    if test_suites_set_list == None:
                        test_suites_selected.append(test_suite)
                        test_suite_names_selected.append(test_suite_name)
                    elif len(test_suites_set_list) == 0:
                        test_suites_selected.append(test_suite)
                        test_suite_names_selected.append(test_suite_name)
                    elif test_suite_name in test_suites_set_list:
                        test_suites_selected.append(test_suite)
                        test_suite_names_selected.append(test_suite_name)
                    else:
                        pass
    logger.info(f"test_suite_names_selected = {test_suite_names_selected}")

    return {
        "test_suite_names_selected": test_suite_names_selected,
        "test_suites_selected": test_suites_selected,
    }


def rockets_context(config_file=None, tests_file_path=None, docker_compose_file=None):
    test_suites = []
    test_suite_names_selected = []
    test_suites_selected = []
    test_suite_set_dict = {}
    test_suites_selected_sets = []  # a list of tuple of (test_suite, test_result_queue)
    test_suite_query_reulst_queue_list = (
        []
    )  # a list of map of test_sutie_name and test_suite_query_result_queue
    proton_setting = os.getenv("PROTON_SETTING", "default")

    root_logger = logging.getLogger()
    logger.info(f"rockets_run starts..., root_logger.level={root_logger.level}")
    if root_logger.level != None and root_logger.level == 20:
        logging_level = "INFO"
    else:
        logging_level = "DEBUG"

    config = rockets_env_var_get()
    if config == None:
        with open(config_file) as f:
            configs = json.load(f)
        logger.debug(f"rockets_context: config reading from config files: {config}")
        config = configs.get(proton_setting)
        logger.debug(f"setting = {proton_setting},config = {config}")

    if config == None:
        raise Exception("No config env vars nor config file")

    res_scan_tests_file_path = scan_tests_file_path(tests_file_path)
    # logger.debug(f"res_scan_tests_file_path = {res_scan_tests_file_path}")
    test_suite_names_selected = res_scan_tests_file_path.get(
        "test_suite_names_selected"
    )
    test_suites_selected = res_scan_tests_file_path.get("test_suites_selected")
    logger.debug(f"test_suite_names_selected = {test_suite_names_selected}")

    test_suite_run_ctl_queue = (
        mp.JoinableQueue()
    )  # queue for rockets_run and test_sute_runner ctrl communication
    test_suite_result_done_queue = (
        mp.JoinableQueue()
    )  # queue for rockets_run get test_suite_result_summary that complete query_results_done test_suite_runner

    for suite in test_suites_selected:

        test_suite_name = suite.get("test_suite_name")

        query_results_queue = mp.JoinableQueue()
        test_suite_query_reulst_queue_list.append(
            {test_suite_name: query_results_queue}
        )

        # test_suite_run_ctl_queue = mp.JoinableQueue()
        # test_suite_result_queue = mp.JoinableQueue()
        # query_result_queue = mp.Queue() # create result queue for each test_sute_run and query_execute pair
        alive = mp.Value("b", True)

        test_suite_set_dict = {
            "test_suite_name": test_suite_name,
            "test_suite": suite,
            "test_suite_run_ctl_queue": test_suite_run_ctl_queue,
            "test_suite_result_done_queue": test_suite_result_done_queue,
            # "query_exe_parent_conn": query_exe_parent_conn,
            # "query_exe_child_conn": query_exe_child_conn,
            "query_results_queue": query_results_queue,
            "alive": alive,
            "logging_level": logging_level,
        }

        test_suites_selected_sets.append(test_suite_set_dict)

    # tests = test_suite.get("tests")

    # query_exe_client = mp.Process(target=query_execute, args=(config, query_exe_child_conn, query_result_list)) # Create query_exe_client process
    # query_exe_queue = mp.Queue() #control path queue for query statements, rockets_run pushes statements into the queue.
    # query_results_queue = (
    #    mp.Queue()
    # )  # data path queue for query results, query_execute and threads created by query_execute process pushed query results into this queue.

    # query_exe_client = mp.Process(target=query_execute_new, args=(config, query_exe_queue, query_results_queue))

    rockets_context = {
        "proton_setting":proton_setting,
        "config": config,
        "test_suite_run_ctl_queue": test_suite_run_ctl_queue,
        "test_suite_result_done_queue": test_suite_result_done_queue,
        "test_suite_query_result_queue_list": test_suite_query_reulst_queue_list,
        "test_suite_names_selected": test_suite_names_selected,
        "test_suites_selected_sets": test_suites_selected_sets,
        "docker_compose_file": docker_compose_file,
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


def kill_query(proton_client, query_2_kill, logging_level="INFO"):
    # currently only stream query kill logic is done, query_2_kill is the query_id, get the id and kill the query and recv the stream query results from query_execute

    logger = mp.get_logger()
    # console_handler = logging.StreamHandler(sys.stderr)
    # console_handler.formatter = formatter
    # logger.addHandler(console_handler)

    # if logging_level == "INFO":
    #    logger.setLevel(logging.INFO)
    # else:
    #    logger.setLevel(logging.DEBUG)
    logger.debug(
        f"kill_query starts, logger={logger}, logger.handler = {logger.handlers}, logger.level = {logger.level}"
    )
    kill_sql = f"kill query where query_id = '{query_2_kill}'"
    # run the timer and then kill the query

    logger.debug(
        f"kill_query: datetime.now = {datetime.datetime.now()}, kill_sql = {kill_sql} to be called."
    )
    kill_res = proton_client.execute(kill_sql)
    logger.info(
        f"kill query_id = {query_2_kill}: kill_sql = {kill_sql} cmd executed, kill_res = {kill_res} was called"
    )
    while len(kill_res):
        time.sleep(0.2)
        kill_res = proton_client.execute(kill_sql)
        logger.debug(f"kill_query: kill_res = {kill_res} was called")
    logger.info(
        f"kill query_id = {query_2_kill}, kill_sql = {kill_sql} cmd executed and success."
    )


def request_rest(
    http_method,
    url,
    rest_type="raw",
    params=None,
    data=None,
    headers=None,
    cookies=None,
    files=None,
    auth=None,
    timeout=None,
    allow_redirects=True,
    proxies=None,
    hooks=None,
    stream=None,
    verify=None,
    cert=None,
    json_obj=None,
):
    logger = mp.get_logger()
    logger.debug(
        f"rest_type={rest_type}, url={url}, http_method={http_method}, params={params}, data={data}"
    )
    try:
        if rest_type == "raw":
            if data != None:
                data = json.dumps(data)

        res = requests.request(
            http_method,
            url,
            params=params,
            data=data,
            headers=headers,
            cookies=cookies,
            files=files,
            auth=auth,
            timeout=timeout,
            allow_redirects=allow_redirects,
            proxies=proxies,
            hooks=hooks,
            stream=stream,
            verify=verify,
            cert=cert,
            json=json_obj,
        )
        logger.debug(f"res.status_code = {res.status_code}")
        return res
    except (BaseException) as error:
        logger.debug(f"exception, error= {error}")
        return None


def is_json(string):
    try:
        json_obj = json.loads(string)
        return True
    except (ValueError) as error:
        return False


def exec_command(command, timeout=2):
    logger = mp.get_logger()
    ret = subprocess.run(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=timeout,
    )
    logger.debug(f"ret of subprocess.run({command}) = {ret}")

    return ret.returncode


def query_run_exec(statement_2_run, config):
    logger = mp.get_logger()
    logger.debug(
        f"local running: handler of logger = {logger.handlers}, logger.level = {logger.level}"
    )
    query_results = {}
    query_id = str(statement_2_run.get("query_id"))
    query_type = statement_2_run.get("query_type")
    query = statement_2_run.get("query")
    query_client = statement_2_run.get("client")
    depends_on_stream = statement_2_run.get("depends_on_stream")
    query_start_time_str = str(datetime.datetime.now())
    query_end_time_str = str(datetime.datetime.now())
    user = statement_2_run.get("user")
    password = statement_2_run.get("password")
    query_result_str = ""
    query_result_column_types = []
    query_result_json = {}
    query_result_list = []
    query_results = {}
    rest_request = ""
    proton_server_container_name = config.get("proton_server_container_name")
    proton_server_container_name_list = proton_server_container_name.split(",")
    rest_setting = config.get("rest_setting")
    table_ddl_url = rest_setting.get("table_ddl_url")
    # if len(proton_server_container_name_list) == 1:
    #     command = f'docker exec {proton_server_container_name} proton-client -u {user} --password {password} --query="{query}"'
    # else:
    commands = []
    for proton_server_container_name in proton_server_container_name_list:
        command = f'docker exec {proton_server_container_name} proton-client -u {user} --password {password} --query="{query}"'
        commands.append(command)
    logger.debug(f"commands = {commands}")
    try:
        if depends_on_stream != None:
            proton_server = config.get("proton_server")
            # proton_server_native_port = config.get("proton_server_native_port")
            # settings = {"max_block_size": 100000}
            # logger.debug(f"depends_on_stream = {depends_on_stream}, checking...")
            retry = 500
            # pyclient = Client(
            #     host=proton_server, port=proton_server_native_port
            # )  # create python client
            # while not table_exist_py(pyclient, depends_on_stream):
            while not table_exist(table_ddl_url, depends_on_stream):
                time.sleep(0.01)
                retry -= 1
            if retry > 0:
                logger.debug(f"depends_on_stream exists.")
            else:
                logger.debug(f"depends_on_stream does not exist, raise exception")
                raise Exception(
                    f"depends_on_stream = {depends_on_stream} for query_id = {query_id}, query = {query} not found"
                )

        # query_result_str = exec_command(command)
        query_result_str = ""
        for command in commands:
            query_result_str += exec_command(command)
        query_end_time_str = str(datetime.datetime.now())
        query_results = {
            "query_id": query_id,
            "query_client": query_client,
            "query": query,
            "rest_request": rest_request,
            "query_type": query_type,
            "query_state": "run",
            "query_start": query_start_time_str,
            "query_end": query_end_time_str,
            "query_result_column_types": query_result_column_types,
            "query_result": query_result_str,
        }
    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")
        query_end_time_str = str(datetime.datetime.now())
        query_results = {
            "query_id": query_id,
            "query_client": query_client,
            "query": query,
            "rest_request": rest_request,
            "query_type": query_type,
            "query_state": "exception",
            "query_start": query_start_time_str,
            "query_end": query_end_time_str,
            "query_result": f"error_code:{error.code}",
        }
        logger.debug(
            "db exception, none-cancel query_results: {}".format(query_results)
        )
    finally:
        logger.debug(f"query_results = {query_results}")
        return query_results


def query_run_rest(rest_setting, statement_2_run):
    logger = mp.get_logger()
    logger.debug(
        f"local running: handler of logger = {logger.handlers}, logger.level = {logger.level}"
    )
    query_results = {}
    query_id = str(statement_2_run.get("query_id"))
    query_type = statement_2_run.get("query_type")
    query = statement_2_run.get("query")
    depends_on_stream = statement_2_run.get("depends_on_stream")
    query_start_time_str = str(datetime.datetime.now())
    query_end_time_str = str(datetime.datetime.now())
    query_result_str = ""
    query_result_column_types = []
    query_result_json = {}
    query_result_list = []
    query_result = ""
    query_results = {}
    rest_request = ""
    try:
        host_url = rest_setting.get("host_url")
        http_snapshot_url = rest_setting.get("http_snapshot_url")
        rest_type = statement_2_run.get("rest_type")
        query_url = statement_2_run.get("query_url")
        table_ddl_url = rest_setting.get("table_ddl_url")
        wait = statement_2_run.get("wait")
        http_method = statement_2_run.get("http_method")
        data = statement_2_run.get("data")
        args = statement_2_run.get("args")

        # When query ID is specified in REST case, honor it
        headers = statement_2_run.get("headers")
        if headers:
            headers["x-proton-query-id"] = query_id
        else:
            headers = {"x-proton-query-id": query_id}

        params = statement_2_run.get("params")
        depends_on = statement_2_run.get("depends_on")

        if depends_on_stream != None:
            logger.debug(f"depends_on_stream = {depends_on_stream}, checking...")
            retry = 500
            while not table_exist(table_ddl_url, depends_on_stream):
                time.sleep(0.01)
                retry -= 1
            if retry > 0:
                logger.debug(f"depends_on_stream exists.")
            else:
                logger.debug(f"depends_on_stream does not exist, raise exception")
                raise Exception(
                    f"depends_on_stream = {depends_on_stream} for input not found"
                )
        # if params == None: params = rest_setting.get("params")

        if depends_on != None:
            depends_on_exists = False
            depends_on_exists = query_exists(depends_on, query_url)
            if not depends_on_exists:
                logger.debug(
                    f"depends_on = {depends_on} of query_id = {query_id} does not exist, raise exception"
                )
                raise Exception(
                    f"depends_on = {depends_on} of query_id = {query_id} does not exist, raise exception"
                )

        if rest_type == "raw":
            url = host_url + query_url
        else:
            url = http_snapshot_url + query_url
        rest_request = f"url={url},http_method={http_method}, rest_type = {rest_type}, params={params},data={data}"

        if wait != None:
            wait = int(wait)
            print(f"wait for {wait} to start run query = {query}")
            time.sleep(wait)

        logger.debug(f"rest_request({rest_request}) to be called.")
        res = request_rest(http_method, url, rest_type, params, data, headers)
        query_end_time_str = str(datetime.datetime.now())
        logger.debug(
            f"rest_request({rest_request}) called, res.status_code = {res.status_code}."
        )

        if is_json(res.text):
            query_result = res.json()
            logger.debug(
                f"res.status_code={res.status_code}, query_result=res.jon()={query_result}"
            )
        else:
            query_result = res.text
            logger.debug(
                f"res.status_code={res.status_code}, query_result=res.text={query_result}"
            )
        query_results = {
            "query_id": query_id,
            "query": query,
            "rest_type": rest_type,
            "rest_request": rest_request,
            "query_type": query_type,
            "query_state": "run",
            "query_start": query_start_time_str,
            "query_end": query_end_time_str,
            "query_result_column_types": query_result_column_types,
            "query_result": query_result,
        }

    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")
        query_end_time_str = str(datetime.datetime.now())
        query_results = {
            "query_id": query_id,
            "query": query,
            "rest_type": rest_type,
            "rest_request": rest_request,
            "query_type": query_type,
            "query_state": "exception",
            "query_start": query_start_time_str,
            "query_end": query_end_time_str,
            "query_result": f"error_code:{error.code}",
        }
        logger.debug(
            "db exception, none-cancel query_results: {}".format(query_results)
        )
    finally:
        logger.debug(f"query_results = {query_results}")
        return query_results


def query_run_py(
    statement_2_run,
    settings,
    query_results_queue=None,
    config=None,
    pyclient=None,
    telemetry_shared_list=None,
    logging_level="INFO",
):
    query_run_start = datetime.datetime.now()
    # logger = logging.getLogger(__name__)
    # logger = mp.get_logger()
    # logger.debug(f"query_run_py: handler of logger = {logger.handlers}")
    # console_handler = logging.StreamHandler(sys.stderr)
    # logger.debug(f"query_run_py: handler of logger = {logger.handlers}")
    # console_handler.formatter = formatter
    # logger.addHandler(console_handler)
    # logger.debug(f"query_run_py starts, logging_level = {logging_level}")
    # logger.debug(f"query_run_py: handler of logger = {logger.handlers}")
    # logger.debug(f"query_run_py starts...")
    # time.sleep(300)
    # if logging_level=="INFO":
    #    logger.setLevel(logging.INFO)
    # else:
    #    logger.setLevel(logging.DEBUG)

    try:
        if pyclient == None:
            logger = mp.get_logger()
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.formatter = formatter
            logger.addHandler(console_handler)
            if logging_level == "INFO":
                logger.setLevel(logging.INFO)
            else:
                logger.setLevel(logging.DEBUG)
            logger.debug(
                f"process started: handler of logger = {logger.handlers}, logger.level = {logger.level}"
            )
            proton_server = config.get("proton_server")
            proton_server_native_ports = config.get("proton_server_native_port")
            proton_server_native_ports = proton_server_native_ports.split(",")
            proton_server_native_port = proton_server_native_ports[
                0
            ]  # todo: get proton_server and port from statement
            settings = {"max_block_size": 100000}
            pyclient = Client(
                host=proton_server, port=proton_server_native_port
            )  # create python client
            CLEAN_CLIENT = True
        else:
            logger = mp.get_logger()
            logger.debug(
                f"local running: handler of logger = {logger.handlers}, logger.level = {logger.level}"
            )

        #logger.debug(f"config = {config}")

        rest_setting = config.get("rest_setting")
        table_ddl_url = rest_setting.get("table_ddl_url")
        query = statement_2_run.get("query")
        query_id = str(statement_2_run.get("query_id"))
        query_type = statement_2_run.get("query_type")
        iter_wait = statement_2_run.get(
            "iter_wait"
        )  # for some slow table query like test_id=61 in materialized_view
        run_mode = statement_2_run.get("run_mode")
        depends_on_stream = statement_2_run.get("depends_on_stream")
        depends_on = statement_2_run.get("depends_on")
        query_start_time_str = str(datetime.datetime.now())
        query_end_time_str = str(datetime.datetime.now())
        element_json_str = ""
        query_result_str = ""
        query_result_column_types = []
        query_result_list = []

        logger.debug(
            f"query_run_py: query_id = {query_id}, query = {query} to be execute @ {str(datetime.datetime.now())}........."
        )

        streams = pyclient.execute("show streams")
        logger.debug(f"show streams = {streams}")

        if depends_on_stream != None and isinstance(depends_on_stream, str):
            retry = 500
            # while not table_exist_py(pyclient, depends_on_stream) and retry > 0:
            while not table_exist(table_ddl_url, depends_on_stream) and retry > 0:
                time.sleep(0.02)
                retry -= 1
            logger.debug(f"retry remains after retry -=1: {retry}")
            if retry <= 0:
                logger.debug(
                    f"check depends_on_stream 500 times and depends_on_stream={depends_on_stream} does not exist"
                )
                raise Exception(
                    f"depends_on_stream = {depends_on_stream} for query_id = {query_id}, query = {query} not found"
                )
            else:
                logger.debug(
                    f"check depends_on_stream, depends_on_stream={depends_on_stream} found."
                )

        if depends_on != None:
            depends_on_exists = False
            depends_on_exists = query_exists(depends_on, client=pyclient)
            if not depends_on_exists:
                logger.debug(
                    f"depends_on = {depends_on} of query_id = {query_id} does not exist, raise exception"
                )
                raise Exception(
                    f"depends_on = {depends_on} of query_id = {query_id} does not exist, raise exception"
                )

        query_result_iter = pyclient.execute_iter(
            query, with_column_types=True, query_id=query_id, settings=settings
        )

        logger.debug(
            f"query_run_py: query_run_py: query_id = {query_id}, executed @ {str(datetime.datetime.now())}, query = {query}......"
        )

        if (query_type != None and query_type == "table") and (iter_wait != None):
            iter_wait = int(iter_wait)
            time.sleep(
                iter_wait
            )  # sleep for materialized_view test_id = 61, the execute_iter is async way, if too quick to start to iter, wait for 1s until query is setup, need to observe
            logger.debug(
                f"query_type = {query_type}, sleep for iter_wait = {iter_wait}s"
            )
        i = 0
        for element in query_result_iter:
            logger.debug(
                f"element got @ {str(datetime.datetime.now())} in query_result_iter in query_id: {query_id} = {element}"
            )

            if isinstance(element, list) or isinstance(element, tuple):
                element = list(element)
                #element.append({"timestamp": str(datetime.datetime.now())})

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
        logger.info(f"query_run_py: query_results of query={query} = {query_results}")

        if query_results_queue != None:
            message_2_send = json.dumps(query_results)
            query_results_queue.put(message_2_send)

        if run_mode == "process" or query_type == "stream":
            # logger.debug(f"query_run_py: query_id = {query_id}, query={query}, query_results = {query_results}")
            query_run_complete = datetime.datetime.now()
            time_spent = query_run_complete - query_run_start
            time_spent_ms = time_spent.total_seconds() * 1000
            if telemetry_shared_list != None:
                telemetry_shared_list.append(
                    {"statement_2_run": statement_2_run, "time_spent": time_spent_ms}
                )
            else:
                print()  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.

            pyclient.disconnect()

    except (BaseException, errors.ServerException) as error:
        logger.debug(f"exception, query_id={query_id}, query={query}, error = {error}")
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
                logger.debug(
                    "query_run_py: query_results: {} collected from query_result_iter at {}".format(
                        query_results, datetime.datetime.now()
                    )
                )
                message_2_send = json.dumps(query_results)
                if query_results_queue != None:
                    query_results_queue.put(message_2_send)
                    logger.info(
                        f"query_run_py: query_results message_2_send = {message_2_send} was sent."
                    )

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
                logger.debug(
                    "query_run_py: db exception, none-cancel query_results: {}".format(
                        query_results
                    )
                )
                message_2_send = json.dumps(query_results)
                if query_results_queue != None:
                    query_results_queue.put(message_2_send)

                # query_result_list = []
                # client.disconnect()

            if run_mode == "process" or query_type == "stream":
                logger.debug(
                    f"query_run_py: query_id = {query_id}, query={query}, query_results = {query_results}"
                )
                query_run_complete = datetime.datetime.now()
                time_spent = query_run_complete - query_run_start
                time_spent_ms = time_spent.total_seconds() * 1000
                if telemetry_shared_list != None:
                    telemetry_shared_list.append(
                        {
                            "statement_2_run": statement_2_run,
                            "time_spent": time_spent_ms,
                        }
                    )
                else:
                    print()  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.

                pyclient.disconnect()

        else:
            query_results = {
                "query_id": query_id,
                "query": query,
                "query_type": query_type,
                "query_state": "exception",
                "query_start": query_start_time_str,
                "query_end": query_end_time_str,
                "query_result": f"error_code:10000, error: {error}",
            }
            # if it's not db excelption, send 10000 as error_code
            message_2_send = json.dumps(query_results)
            if query_results_queue != None:
                query_results_queue.put(message_2_send)

        if run_mode == "process" or query_type == "stream":
            logger.debug(
                f"query_run_py: query_id = {query_id}, query={query}, query_results = {query_results}"
            )
            query_run_complete = datetime.datetime.now()
            time_spent = query_run_complete - query_run_start
            time_spent_ms = time_spent.total_seconds() * 1000
            if telemetry_shared_list != None:
                telemetry_shared_list.append(
                    {"statement_2_run": statement_2_run, "time_spent": time_spent_ms}
                )
            else:
                print()  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.
            pyclient.disconnect()

    finally:

        return query_results


def query_execute(config, child_conn, query_results_queue, alive, logging_level="INFO"):
    mp_mgr = (
        None  # multiprocess manager, will be created when loading query_run_py process
    )
    logger = mp.get_logger()
    # formatter = logging.Formatter(
    #    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s"
    # )

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    if logging_level == "INFO":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    logger.debug(
        f"query_execute starts, logging_level = {logging_level}, logger.handlers = {logger.handlers}"
    )
    telemetry_shared_list = []  # telemetry list for query_run timing
    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_ports = config.get("proton_server_native_port")
    proton_server_native_ports = proton_server_native_ports.split(",")
    proton_server_native_port = proton_server_native_ports[
        0
    ]  # todo: support assign proton_server/port from statement, that means client need to be created inside query_run_py but not in query_execute
    proton_admin = config.get("proton_admin")
    proton_admin_name = proton_admin.get("name")
    proton_admin_password = proton_admin.get("password")
    settings = {"max_block_size": 100000}
    query_result_str = None
    tear_down = False
    query_run_count = 1000  # limit the query run count to 1000,
    query_result_list = []
    # max query_run_count, hard code right now, could be sent from query_walk_through (for example based on total statements no.)
    logger.debug(
        f"proton_server = {proton_server},proton_server_native_port = {proton_server_native_port}"
    )
    client = Client(
        host=proton_server, port=proton_server_native_port
    )  # create python client

    i = 0  # query
    query_procs = []  # a list of query_run_py processes started for queries of one case
    auto_terminate_queries = []
    print(f"query_execute: alive = {alive}, alive.value = {alive.value}")
    while (not tear_down) and query_run_count > 0 and alive.value:
        try:

            query_proc = None

            logger.debug(
                f"query_execute: tear_down = {tear_down}, query_run_count = {query_run_count}, wait for message from test_suite_run......"
            )
            logger.debug(f"child_conn = {child_conn}")
            message_recv = child_conn.recv()
            logger.debug(f"query_execute: message_recv = {message_recv}")

            if message_recv == "tear_down":
                tear_down = True
                break

            elif message_recv == "test_steps_done":
                logger.debug(
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
                        if process.exitcode != None:
                            i += 1
                        # else:
                        elif terminate == "auto":
                            logger.debug(
                                f"query_id = {query_id}, process = {process}, process.exitcode = {process.exitcode}"
                            )
                            retry = 500
                            while (
                                not query_id_exists_py(client, query_id)
                                and process.exitcode == None
                                and retry > 0
                            ):  # process.exitcode is checked when another retry to identify if the query_run_py if already exist due to exception or other reasons.
                                time.sleep(0.05)
                                retry -= 1
                            if retry > 0:
                                logger.debug(
                                    f"check and wait for query exist to kill, query_2_kill = {query_id} is found, continue"
                                )
                            else:
                                logger.debug(
                                    f"check and wait for query exist to kill, query_2_kill = {query_id} after 500 times with 0.05s sleep retring check still not found, continue."
                                )

                            if query_end_timer != None:
                                logger.debug(
                                    f"query_end_timer = {query_end_timer}, sleep {query_end_timer} secons."
                                )
                                time.sleep(int(query_end_timer))
                            logger.debug(
                                f"query_end_timer sleep {query_end_timer} end start to call kill_query..."
                            )
                            kill_query(client, query_id)
                            logger.debug(
                                f"kill_query() was called, query_id={query_id}"
                            )
                        logger.debug(
                            f"query_execute: query_procs = {query_procs} after trying to remove"
                        )
                    time.sleep(0.2)

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
            # elif message_recv == "case_result_done":
            # message_2_send = "case_result_done"
            # query_exe_queue.put(message_recv)
            # query_run_count -= 1
            else:
                statement_2_run = json.loads(json.dumps(message_recv))
                logger.debug(f"query_execute: statement_2_run = {statement_2_run}")
                query_id = str(statement_2_run.get("query_id"))
                query_client = statement_2_run.get("client")
                query_type = statement_2_run.get("query_type")
                user_name = statement_2_run.get("user")
                password = statement_2_run.get("password")
                terminate = statement_2_run.get("terminate")
                if terminate == "auto":
                    auto_terminate_queries.append(statement_2_run)
                run_mode = statement_2_run.get("run_mode")
                wait = statement_2_run.get("wait")
                query = statement_2_run.get("query")
                query_end_timer = statement_2_run.get("query_end_timer")
                query_start_time_str = str(datetime.datetime.now())
                query_end_time_str = str(datetime.datetime.now())

                if run_mode == "process" or query_type == "stream":
                    mp_mgr = mp.Manager()  # create a multiprocess.Manager object
                    telemetry_shared_list = mp_mgr.list()

                    query_run_args = (
                        statement_2_run,
                        settings,
                        query_results_queue,
                        config,
                        None,
                        telemetry_shared_list,
                        logging_level,
                    )
                    if wait != None:
                        wait = int(wait)
                        print(
                            f"query_execute: wait for {wait} to start run query = {query}"
                        )
                        time.sleep(wait)

                    query_proc = mp.Process(target=query_run_py, args=query_run_args)
                    query_procs.append(
                        {
                            "process": query_proc,
                            "terminate": terminate,
                            "query_end_timer": query_end_timer,
                            "query_id": query_id,
                            "query": query,
                        }  # have to put append before start, otherwise exception when append shared list.
                    )  # put every query_run process into array for case_done check
                    query_proc.start()

                    logger.debug(
                        f"query_execute: start a proc for query = {query}, query_run_args = {query_run_args}, query_proc.pid = {query_proc.pid}"
                    )

                else:
                    if query_client != None and query_client == "rest":
                        logger.debug(
                            f"query_run_rest run local for query_id = {query_id}..."
                        )
                        query_results = query_run_rest(rest_setting, statement_2_run)
                        logger.debug(f"query_id = {query_id}, query_run_rest is called")
                    elif query_client != None and query_client == "exec":
                        logger.debug(
                            f"query_run_exec run local for query_id = {query_id}..."
                        )
                        query_results = query_run_exec(statement_2_run, config)
                        logger.debug(f"query_id = {query_id}, query_run_exec is called")
                    else:
                        logger.debug(
                            f"query_execute: query_run_py run local for query_id = {query_id}..."
                        )
                        if wait != None:
                            wait = int(wait)
                            logger.debug(
                                f"query_id = {query_id}, start wait for {wait}s"
                            )
                            time.sleep(wait)
                            logger.debug(
                                f"query_id = {query_id}, end wait for {wait}s continue"
                            )
                        logger.debug(f"query_id = {query_id}, to call query_run_py")

                        if user_name != None and password != None:
                            statement_client = Client(
                                host=proton_server,
                                port=proton_server_native_port,
                                user=user_name,
                                password=password,
                            )
                            logger.debug(
                                f"statement_client=Client(host={proton_server}, port={proton_server_native_port}, user={user_name}, password={password})"
                            )
                        else:
                            statement_client = client
                            logger.debug(
                                f"statement_client={client}, statement_2_run={statement_2_run}"
                            )

                        query_results = query_run_py(
                            statement_2_run,
                            settings,
                            query_results_queue=None,
                            config=config,
                            pyclient=statement_client,
                        )
                        print(f"query_execute: query_results = {query_results}")

                        logger.debug(f"query_id = {query_id}, query_run_py is called")
                    message_2_send = json.dumps(query_results)
                    query_results_queue.put(message_2_send)
                    logger.debug(
                        f"query_execute: query_run_py run local, query_id = {query_id}, query={query}, message_2_send = {query_results} pushed to query_results_queue "
                    )
                    time.sleep(
                        0.05
                    )  # 0.2 originally, wait for the queue push completed, if no sleep the rockets_rum process got the results_done message too fast and then go to next case, the reulsts in queue will be lost, todo: put a beacon message to indicate the messages of cases done

                query_run_count = query_run_count - 1
        except (BaseException, errors.ServerException) as error:
            logger.debug(f"exception: error = {error}")
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
                logger.debug(
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
            if message_recv == "test_steps_done":
                message_2_send = "case_result_done"
                # query_exe_queue.put(message_2_send)
                child_conn.send(message_2_send)
                query_run_count -= 1
            query_run_count = query_run_count - 1
        finally:
            print()  # todo: some logic here for handling.

            # if query_proc != None:
            #    query_procs.append(
            #        {
            #            "process": query_proc,
            #            "terminate": terminate,
            #            "query_end_timer": query_end_timer,
            #            "query_id": query_id,
            #            "query": query,
            #        }
            #    )  # put every query_run process into array for case_done check
    if query_run_count == 0:
        logger.debug(
            "Super, 1000 queries hit by a single test suite, we are in great time, by James @ Jan 10, 2022!"
        )
    # if len(query_procs) != 0:
    for proc in query_procs:
        process = proc.get("process")
        # process.terminate()
        process.join()
    count = 0  # for avg_spent_time_ms of query_run statistics
    time_spent_query_run_ms = 0
    avg_time_spent_query_run_ms = 0
    logger.debug(f"telemetry_shared_list = {telemetry_shared_list}")
    for item in telemetry_shared_list:
        time_spent_query_run_ms += item.get("time_spent")
        count += 1
    if count != 0:
        avg_time_spent_query_run_ms = time_spent_query_run_ms / count
    else:
        avg_time_spent_query_run_ms = 0
    logger.info(
        f"query_run execute {count} times, total {time_spent_query_run_ms} ms spent, avg_time_spent_query_run_ms = {avg_time_spent_query_run_ms}"
    )
    if mp_mgr != None:
        del mp_mgr
    client.disconnect()
    # query_exe_queue.put("tear_down_done")
    child_conn.send("tear_down_done")
    logger.info(f"query_execute: tear_down completed and end")


def query_walk_through(statements, query_conn):
    # logger.debug(f"query_walk_through: start..., statements = {statements}.")
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
        query = statement.get("query")
        query_type = statement.get("query_type")
        terminate = statement.get("terminate")
        if query_id == None:
            query_id = str(uuid.uuid1())

            # query_id = random.randint(
            #    1, 10000
            # )  # unique query id, if no query_id specified in tests.json
            statement["query_id"] = query_id

        if query_type == "stream" and terminate == None:
            statement[
                "terminate"
            ] = "auto"  # for stream query, by default auto-terminate

        # logger.debug(f"query_walk_through: statement = {statement}.")
        if query_end_timer == None:
            query_end_timer = 0

        # query_exe_queue.put(statement)
        query_conn.send(statement)
        logger.debug(
            # f"query_walk_through: statement query_id = {query_id} was pushed into query_exe_queue."
            f"query_walk_through: statement query_id = {query_id}, query = {query} was send to query_execute."
        )

        if isinstance(wait, dict):  # if wait for a specific query done
            print()  # todo: check the query_id and implement the logic to notify the query_execute that this query need to be done after the query to be wait done and implement the wait logic in query_execute_new
        elif str(wait).isdigit():  # if wait for x seconds and then execute the query
            time.sleep(wait)

        statement_id_run += 1
        # time.sleep(1) # wait the query_execute execute the stream command

    logger.debug(f"query_walk_through: end... stream_query_id = {stream_query_id}")
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
            logger.debug(
                "input_walk_through_pyclient: {} done at {}.".format(
                    input_sql, datetime.datetime.now()
                )
            )
            # time.sleep(1)
            input_results.append(input_result)
        time.sleep(1)  # wait 1s for data inputs completed.
    return input_results


def query_id_exists_py(py_client, query_id, query_exist_check_sql=None):
    logger = mp.get_logger()
    query_id = str(query_id)
    if query_exist_check_sql == None:
        query_exist_check_sql = (
            f"select query_id from system.processes where query_id = '{query_id}'"
        )
    try:
        # logger.debug(f"query_exist_check_sql = {query_exist_check_sql} to be called.")
        res_check_query_id = py_client.execute(query_exist_check_sql)
        logger.debug(f"query_exist_check_sql = {query_exist_check_sql} to was called.")
        logger.debug(f"res_check_query_id = {res_check_query_id}")
        if res_check_query_id != None and isinstance(res_check_query_id, list):
            for element in res_check_query_id:
                logger.debug(f"element = {element}, query_id = {query_id}")
                if query_id in element:
                    return True
            return False
        else:
            logger.debug(f"query_id_list is None or not a list")
            return False
    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")
    return False


def query_id_exists_rest(query_url, query_id, query_body=None):
    logger = mp.get_logger()
    query_id = str(query_id)
    try:
        query_body = json.dumps(
            {
                "query": f"select query_id from system.processes where query_id = '{query_id}'"
            }
        )
        res = requests.post(query_url, data=query_body)
        logger.debug(f"query_id exists check: res.status_code = {res.status_code}")
        if res.status_code != 200:
            return False
        res_json = res.json()
        query_id_list = []
        query_id_list = res_json.get("data")
        logger.debug(f"query_id_list: {query_id_list}")
        if query_id_list == None or not isinstance(query_id_list, list):
            logger.debug(f"query_id_list is None or not a list")
            return False
        else:
            for element in query_id_list:
                logger.debug(f"element = {element}, query_id = {query_id}")
                if query_id in element:
                    return True
            return False
    except (BaseException) as error:
        logger.info(f"exception, error = {error}")
        return False


def query_exists(
    query_id, query_url=None, client=None
):  # todo: adopt query_id_exists_py and query_id_exists_rest
    logger = mp.get_logger()
    query_id_list = []
    query_id_exists = False
    retry = 200
    logger.debug(f"checking query_id = {query_id} if exists...")
    while not query_id_exists and retry > 0:
        if client == None:
            try:
                query_id_exists = query_id_exists_rest(query_url, query_id)
            except (BaseException) as error:
                logger.debug(
                    f"query_exist exception, query_url = {query_url}, query_id = {query_id}, error = {error}"
                )
                raise Exception(
                    f"query_exist exception, query_url = {query_url}, query_id = {query_id}, error = {error}"
                )
        else:
            try:
                query_id_exists = query_id_exists_py(client, query_id)
            except (BaseException) as error:
                logger.debug(
                    f"query_exist exception, query_id = {query_id}, error = {error}"
                )
                raise Exception(
                    f"query_exist exception, query_id = {query_id}, error = {error}"
                )
        if query_id_exists:
            return True
        else:
            time.sleep(0.05)
            retry -= 1

    if query_id_exists:
        return True
    else:
        return False


def input_batch_rest(rest_setting, input_batch, table_schema):
    # todo: complete the input by rest
    logger = mp.get_logger()
    input_batch_record = {}
    try:
        logger.debug(
            f"input_batch_rest: input_batch = {input_batch}, table_schema = {table_schema}"
        )
        input_url = rest_setting.get("ingest_url")
        query_url = rest_setting.get("query_url")
        table_ddl_url = rest_setting.get("table_ddl_url")
        wait = input_batch.get("wait")
        # table_name = table_schema.get("name")
        table_name = input_batch.get("table_name")
        if table_name == None:
            raise Exception("table_name of input_batch is None")
        columns = input_batch.get("columns")
        if columns == None and table_schema == None:
            return []

        retry = 500
        while not table_exist(table_ddl_url, table_name):
            time.sleep(0.01)
            retry -= 1
        input_rest_columns = []
        input_rest_body_data = []
        input_rest_body = {"columns": input_rest_columns, "data": input_rest_body_data}

        depends_on_stream = input_batch.get("depends_on_stream")
        if depends_on_stream != None:
            logger.debug(f"depends_on_stream = {depends_on_stream}, checking...")
            retry = 500
            while not table_exist(table_ddl_url, depends_on_stream):
                time.sleep(0.01)
                retry -= 1
            if retry > 0:
                logger.debug(f"depends_on_stream exists.")
            else:
                logger.debug(
                    f"depends_on_stream = {depends_on_stream} does not exist, raise exception"
                )
                raise Exception(
                    f"depends_on_stream = {depends_on_stream} for input not found"
                )

        depends_on = input_batch.get("depends_on")
        depends_on_exists = False

        if depends_on != None:
            logger.debug(f"depends_on = {depends_on}, checking...")
            depends_on_exists = query_exists(depends_on, query_url)
            if not depends_on_exists:
                logger.debug(
                    f"depends_on = {depends_on} for input does not exist, raise exception"
                )
                raise Exception(f"depends_on = {depends_on} for input not found")
            """
            query_body = json.dumps({"query": "select query_id from system.processes"})
            res = requests.post(query_url, data=query_body)
            res_json = res.json()
            query_id_list = res_json.get("data")
            logger.debug(f"query_id_list: {query_id_list}")
            if query_id_list != None and len(query_id_list) > 0:
                retry = 200
                # depends_on_exists = False
                while retry > 0 and depends_on_exists != True:
                    for element in query_id_list:
                        if depends_on in element:
                            depends_on_exists = True
                            logger.debug(
                                f"depends_on = {depends_on}, element in query_id_list = {element}, matched, depends_on found in query_id_list."
                            )
                    time.sleep(0.05)
                    retry -= 1
                    query_body = json.dumps(
                        {"query": "select query_id from system.processes"}
                    )
                    res = requests.post(query_url, data=query_body)
                    res_json = res.json()
                    query_id_list = res_json.get("data")
                    logger.debug(f"query_id_list: {query_id_list}")
            """
        if wait != None:
            logger.debug(f"wait for {wait}s to start inputs.")
            wait = int(wait)
            logger.info(f"sleep {wait} before input")
            time.sleep(wait)

        logger.debug(
            f"depends_on = {depends_on}, depends_on_exists = {depends_on_exists}"
        )

        if columns != None:
            for each in columns:
                input_rest_columns.append(each)
            logger.debug(
                f"columns in input_batch != None: columns = {columns}, input_rest_columns = {input_rest_columns}, input_batch_rest: input_url = {input_url}, input_rest_body = {input_rest_body}"
            )

        elif table_schema != None:
            for element in table_schema.get("columns"):
                input_rest_columns.append(element.get("name"))
        input_batch_data = input_batch.get("data")
        for row in input_batch_data:
            logger.debug(f"input_batch_rest: row_data = {row}")
            input_rest_body_data.append(
                row
            )  # get data from inputs batch dict as rest ingest body.
        input_rest_body = json.dumps(input_rest_body)
        input_url = f"{input_url}/{table_name}"
        logger.debug(
            f"input_batch_rest: input_url = {input_url}, input_rest_body = {input_rest_body}"
        )

        retry = 200
        while not table_exist(table_ddl_url, table_name):
            time.sleep(0.05)
            logger.debug(
                f"table_name = {table_name} for input does not exit, wait for 1s"
            )
            retry -= 1
        if retry > 0:
            logger.debug(
                f"table_name = {table_name} for input found, continue to call res = requests.post(input_url, data=input_rest_body)"
            )
        else:
            logger.debug(
                f"table_name = {table_name} for input not found after multiple retry, raise exception"
            )
            raise Exception(f"table_name = {table_name} for input not found")

        res = requests.post(input_url, data=input_rest_body)
        logger.debug(
            f"input_batch_rest: response of requests.post of input_url = {input_url}, data = {input_rest_body} request res = {res}"
        )

        assert res.status_code == 200, f"res.status_code = {res.status_code}"
        input_batch_record["input_batch"] = input_rest_body_data
        input_batch_record["timestamp"] = str(datetime.datetime.now())
        logger.debug(f"input_batch done succesfully...")

        """
        if res.status_code != 200:
            logger.debug(f"table input rest access failed, status code={res.status_code}") 
            raise Exception(f"table input rest access failed, status code={res.status_code}")
        else:
            input_batch_record["input_batch"] =input_rest_body_data
            input_batch_record["timestamp"] = str(datetime.datetime.now())
            #logger.debug("input_rest: input_batch {} is inserted".format(input_rest_body_data)) 
        """
        return input_batch_record
    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")
        return input_batch_record


def find_schema(table_name, table_schemas):
    if table_schemas == None:
        return None
    for table_schema in table_schemas:
        if table_name == table_schema.get("name"):
            return table_schema
    return None


def input_walk_through_rest(
    rest_setting,
    inputs,
    table_schemas,
    wait_before_inputs=1,  # todo: remove all the sleep
    sleep_after_inputs=1.5,  # todo: remove all the sleep (current stable set wait_before_inputs=1, sleep_after_inputs=1.5)
):
    logger = mp.get_logger()
    logger.debug(
        f"running here, rest_setting = {rest_setting}, table_schemas = {table_schemas}"
    )
    wait_before_inputs = wait_before_inputs  # the seconds sleep before inputs starts to ensure the query is run on proton.
    sleep_after_inputs = sleep_after_inputs  # the seconds sleep after evary inputs of a case to ensure the stream query result was emmited by proton and received by the query execute
    logger.debug(f"running here.")
    time.sleep(wait_before_inputs)
    input_url = rest_setting.get("ingest_url")
    inputs_record = []

    try:
        for batch in inputs:
            table_name = batch.get("table_name")
            depends_on = batch.get("depends_on")
            table_schema = find_schema(table_name, table_schemas)

            logger.debug(f"input_walk_through_rest: table_schema = {table_schema}")
            batch_sleep_before_input = batch.get("sleep")
            if batch_sleep_before_input != None:
                logger.info(f"sleep {batch_sleep_before_input} before input")
                time.sleep(int(batch_sleep_before_input))

            input_batch_record = input_batch_rest(rest_setting, batch, table_schema)
            inputs_record.append(input_batch_record)

        if isinstance(sleep_after_inputs, int):
            time.sleep(sleep_after_inputs)
    except (BaseException) as error:
        logger.info(f"exception: error = {error}")
    return inputs_record


def drop_table_if_exist_rest(table_ddl_url, table_name):
    logger = mp.get_logger()
    res = requests.get(table_ddl_url)
    if res.status_code == 200:
        res_json = res.json()
        table_list = res_json.get("data")
        #logger.debug(f"table_list = {table_list}")
        if table_list:
            for element in table_list:
                if element.get("name") == table_name:
                    res = requests.delete(f"{table_ddl_url}/{table_name}")
                    drop_start_time = datetime.datetime.now()
                    if res.status_code != 200:
                        raise Exception(
                            f"drop stream rest = {table_ddl_url}/{table_name} access failed, status code={res.status_code} while stream exists:{table_list}"
                        )
                    else:
                        # time.sleep(1)  # sleep to wait the table drop completed.
                        logger.info(
                            "drop stream {} is successfully called".format(table_name)
                        )
                        wait_times = 0
                        while table_exist(table_ddl_url, table_name):
                            time.sleep(0.2)
                            wait_times += 1
                        # wait_time = wait_times * 10
                        drop_complete_time = datetime.datetime.now()
                        time_spent = drop_complete_time - drop_start_time
                        time_spent_ms = time_spent.total_seconds() * 1000
                        logger.info(f"drop stream {table_name} is successfully")
                        logger.info(f"{time_spent_ms} ms spent on {table_name} drop")
                        global TABLE_DROP_RECORDS
                        TABLE_DROP_RECORDS.append(
                            {"table_name": {table_name}, "time_spent": time_spent_ms}
                        )
        else:
            logger.debug(
                f"table_list is [], table {table_name} does not exit, drop table {table_name} bypass"
            )
    else:
        raise Exception(
            f"table list rest access failed,requests.get({table_ddl_url}) status code={res.status_code}, res_json = {res.json()}"
        )


def table_exist_py(pyclient, table_name):
    logger = mp.get_logger()
    sql_2_run = "show streams"
    try:
        res = pyclient.execute(sql_2_run)
        logger.debug(f"show streams = {res}")
        for element in res:
            if table_name in element:
                return True
        return False
    except (BaseException) as error:
        logger.info(f"exception, error = {error}")
        return False


"""
def table_exist_py(pyclient, table_name):
    table_list = pyclient.execute("show streams")
    for item in table_list:
        if item[0] == table_name:
            logger.debug(
                f"table_name = {table_name} = {item[0]} in table_list of show streams"
            )
            return True
    return False
"""


def table_exist(table_ddl_url, table_name):
    logger = mp.get_logger()
    logger.debug(
        f"table_exist: table_ddl_url = {table_ddl_url}, table_name = {table_name}"
    )
    res = requests.get(table_ddl_url)
    logger.debug(f"table_exist: res.status_code = {res.status_code}")
    if res.status_code == 200:
        res_json = res.json()
        table_list = res_json.get("data")
        #logger.debug(f"table_list = {table_list}")
        if len(table_list) > 0:
            for element in table_list:
                element_name = element.get("name")
                # logger.debug(f"table_exist: element_name = {element_name}, table_name = {table_name}")
                if element_name == table_name:
                    logger.debug(f"table_exist: table_name = {table_name} exists.")
                    return True
            logger.debug(f"table_name = {table_name} does not exist")
            return False
        else:
            return False
            # pyclient = Client('localhost', port=8463) # use table_exist_py as an backup in case rest is broken, todo: remove this due to buggy in test runner in different machine than proton-server
            # if table_exist_py(pyclient, table_name):
            #     return True
            # logger.debug("table_list is [] table_name = {table_name} does not exist.")
            # return False
    else:
        logger.debug(
            f"table list rest access failed, requests.get({table_ddl_url}), status code={res.status_code},res_json = {res.json()}"
        )
        raise Exception(
            f"table list rest access failed, requests.get({table_ddl_url}), status code={res.status_code}, res_json = {res.json()}"
        )


def create_table_rest(table_ddl_url, table_schema, retry=3):
    while retry > 0:
        res = None
        try:
            logger.debug(f"create_table_rest starts...")
            table_name = table_schema.get("name")
            type = table_schema.get("type")
            query_parameters = table_schema.get("query_parameters")
            if query_parameters != None:
                table_create_url = table_ddl_url + "?" + query_parameters
            else:
                table_create_url = table_ddl_url
            if type != None:
                table_schema.pop("type")  # type is not legal key/value for rest api
            event_time_column = table_schema.get("event_time_column")
            ttl_expression = table_schema.get("ttl_expression")
            columns = table_schema.get("columns")
            table_schema_for_rest = {"name": table_name, "columns": columns}
            if event_time_column is not None:
                table_schema_for_rest["event_time_column"] = event_time_column
            if ttl_expression is not None:
                table_schema_for_rest["ttl_expression"] = ttl_expression

            post_data = json.dumps(table_schema_for_rest)

            logger.debug(
                f"table_create_url = {table_create_url}, data = {post_data} to be posted."
            )
            # res = requests.post(
            #    table_ddl_url + "?distributed_ingest_mode=sync", data=post_data
            # )  # create the table w/ table schema

            res = requests.post(
                table_create_url, data=post_data
            )  # create the table w/ table schema

            create_start_time = datetime.datetime.now()

            if res.status_code == 200:
                logger.info(f"table {table_name} create_rest is called successfully.")
                break
            else:
                logger.info(
                    f"table {table_name} create_rest fails, res.status_code = {res.status_code}"
                )
                retry -= 1
                if retry <= 0:
                    return res
                time.sleep(1)
                continue
        except (BaseException) as error:
            logging.debug(f"exception: error = {error}")

    create_table_time_out = 1000  # set how many times wait and list table to check if table creation completed.
    while create_table_time_out > 0:
        if table_exist(table_ddl_url, table_name):
            logger.info(f"table {table_name} is created successfully.")
            create_complete_time = datetime.datetime.now()
            time_spent = create_complete_time - create_start_time
            time_spent_ms = time_spent.total_seconds() * 1000
            # time_spent_ms = (1000-create_table_time_out) * 10
            logger.info(f"{time_spent_ms} ms spent on table {table_name} creating")
            global TABLE_CREATE_RECORDS
            TABLE_CREATE_RECORDS.append(
                {"table_name": table_name, "time_spent": time_spent_ms}
            )
            break
        else:
            time.sleep(0.01)
            # res = requests.post(table_ddl_url, data=json.dumps(table_schema)) #currently the health check rest is not accurate, retry here and remove later
        create_table_time_out -= 1
    # time.sleep(1) # wait the table creation completed
    return res


"""
def compose_up(compose_file_path):
    logger.debug(f"compose_up: compose_file_path = {compose_file_path}")
    try:
        cmd = f"docker-compose -f {compose_file_path} up -d"
        logger.debug(f"compose_up: cmd = {cmd}")
        res = subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError) as Error:
        return False
"""


def env_health_check(health_check_url):
    try:
        logger.debug(f"env_health_check: health_check_url = {health_check_url}")
        res = requests.get(health_check_url)
        if res.status_code == 200:
            return True
        else:
            return False
    except (BaseException):
        return False


def create_view_if_not_exit_py(client, table_schema):
    table_type = table_schema.get("type")
    table_name = table_schema.get("name")
    if not table_exist_py(client, table_name):
        if table_type == "view":
            sql_2_run = table_schema.get("create_sql")
            if sql_2_run != None:
                logger.debug(f"create_view_pyclient: sql_2_run = {sql_2_run}")
            else:
                sql_2_run = f"drop view {table_name}"
            res_drop = client.execute(sql_2_run)
            logger.debug(f"create_view_pyclient: executed")
            retry = 100
            while retry < 100 and table_exist_py(client, table_name):
                time.sleep(0.05)
                retry -= 1
            if not table_exist_py(client, table_name):
                logger.debug(f"create view {table_name} failed.")
                return False
            else:
                logger.debug(f"create view {table_name} success.")
                return True
    else:
        logger.debug(f"{table_name} exist, bapass create view")
        return None


def drop_table_if_exist_pylient(client, table_schema):
    table_type = table_schema.get("type")
    table_name = table_schema.get("name")
    if table_type == "view":
        sql_2_run = f"drop view if exists {table_name}"
        logger.debug(f"drop_table_if_exist_pyclient: sql_2_run = {sql_2_run}")
        client.execute(sql_2_run)
        logger.debug(f"drop_table_if_exist_pyclient: view {table_name} droped")


def drop_view_if_exist_py(client, table_name):
    if table_exist_py(client, table_name):
        sql_2_run = f"drop view {table_name}"
        res_drop = client.execute(sql_2_run)
        logger.debug(f"drop view {table_name} is executed, res_drop = {res_drop}")
        retry = 100
        while retry < 100 and table_exist_py(client, table_name):
            time.sleep(0.2)
            count -= 1
        if table_exist_py(client, table_name):
            logger.debug(
                f"drop view {table_name} is failed, table_exist_py({table_name}) = True"
            )
            return False
        else:
            logger.debug(
                f"drop view {table_name} is succesfully, table_exist_py({table_name}) = False"
            )
            return True
    else:
        logger.debug(f"view {table_name} does not exist, bypass drop")
        return None


def test_suite_env_setup(client, config, test_suite_config):
    logger = mp.get_logger()
    rest_setting = config.get('rest_setting')
    if test_suite_config == None:
        return []
    tables_setup = []
    table_ddl_url = rest_setting.get("table_ddl_url")
    params = rest_setting.get("params")
    table_schemas = test_suite_config.get("table_schemas")
    if table_schemas == None:
        table_schemas = []
    for table_schema in table_schemas:
        table_name = table_schema.get("name")
        reset = table_schema.get("reset")
        logger.debug(f"env_setup: table_name = {table_name}, reset = {reset}")

        table_type = table_schema.get("type")
        if reset != None and reset == "False":
            pass
        else:
            if table_type == "table":
                drop_table_res = drop_table_if_exist_rest(table_ddl_url, table_name)
                logger.debug(
                    f"drop_table_if_exist_rest({table_ddl_url}, {table_name}) = {drop_table_res}"
                )
                tables_setup.append(table_name)
            elif table_type == "view":
                #proton_setting = os.getenv("PROTON_SETTING", "default") #todo: get proton_setting from parameter
                drop_view_res = drop_view_if_exist_py(client, table_name)
                #drop_view_res = drop_table_if_exist_rest(table_ddl_url, table_name) #proton bug: https://github.com/timeplus-io/proton/issues/1178
                logger.debug(
                    f"drop_view_if_exist_py(clieent, {table_name}) = {drop_view_res}"
                )
                tables_setup.append(table_name)

    for table_schema in table_schemas:
        table_type = table_schema.get("type")
        table_name = table_schema.get("name")
        # if table_exist_py(client, table_name):
        if table_exist(table_ddl_url, table_name):
            pass
        else:
            if table_type == "table":
                create_table_rest(table_ddl_url, table_schema)
            elif table_type == "view":
                create_view_if_not_exit_py(client, table_schema)

    setup = test_suite_config.get("setup")
    logger.debug(f"setup = {setup}")
    if setup != None:
        setup_inputs = setup.get("inputs")
        if setup_inputs != None:
            logger.debug(f"input_walk_through_rest to be started.")
            setup_input_res = input_walk_through_rest(
                rest_setting, setup_inputs, table_schemas
            )
        setup_statements = setup.get(
            "statements"
        )  # only support table rightnow todo: optimize logic
        if setup_statements != None:
            for statement_2_run in setup_statements:
                settings = {"max_block_size": 100000}
                query_id = statement_2_run.get("query_id")
                query_id = statement_2_run.get("query_id")
                if query_id is None:
                    query_id = str(uuid.uuid1())
                    # query_id = random.randint(
                    #    1, 10000
                    # )  # unique query id, if no query_id specified in tests.json
                statement_2_run["query_id"] = query_id
                query_results = query_run_py(
                    statement_2_run,
                    settings,
                    query_results_queue=None,
                    config=config,
                    pyclient=client,
                )

                logger.debug(f"query_id = {query_id}, query_run_py is called")

    return tables_setup


def env_setup(
    rest_setting,
    env_compose_file=None,
    proton_ci_mode="local",
):
    ci_mode = proton_ci_mode
    logger.info(f"env_setup: ci_mode = {ci_mode}")
    logger.debug(f"env_setup: rest_setting = {rest_setting}")
    health_url = rest_setting.get("health_check_url")
    logger.debug(f"env_setup: health_url = {health_url}")
    tables_cleaned = []
    env_docker_compose_res = True  # todo: remove this return value due to compose_up is done in ci_runner starting phase

    """
    if ci_mode == "local":
        env_docker_compose_res = True
        logger.info(f"Bypass docker compose up.")
    else:
        env_docker_compose_res = compose_up(env_compose_file)
        logger.info(f"env_setup: docker compose up...")
    logger.debug(f"env_setup: env_docker_compose_res: {env_docker_compose_res}")
    
    
    env_health_check_res = env_health_check(health_url)
    logger.info(f"env_setup: env_health_check_res: {env_health_check_res}")
    if env_docker_compose_res:
        retry = 10
        while env_health_check_res == False and retry > 0:
            time.sleep(2)
            env_health_check_res = env_health_check(health_url)
            logger.debug(f"env_setup: retry = {retry}")
            retry -= 1

        if env_health_check_res == False:
            raise Exception("Env health check failure.")
    else:
        raise Exception("Env docker compose up failure.")
    """
    env_health_check_res = env_health_check(health_url)
    logger.info(f"env_setup: env_health_check_res: {env_health_check_res}")
    retry = 10
    while env_health_check_res == False and retry > 0:
        time.sleep(2)
        env_health_check_res = env_health_check(health_url)
        logger.debug(f"env_setup: retry = {retry}")
        retry -= 1

    if env_health_check_res == False:
        raise Exception("Env health check failure.")

    if ci_mode == "github":
        time.sleep(
            10
        )  # health check rest is not accurate, wait after docker compsoe up under github mode, remove later when it's fixed.

    # clean_all_res = clean_all() #todl: drop all the streams and views

    return {
        "env_docker_compose_res": env_docker_compose_res,
        "env_health_check_res": env_health_check_res,
    }


def find_table_reset_in_table_schemas(table, table_schemas):
    if table_schemas == None:
        return []
    for table_schema in table_schemas:
        name = table_schema.get("name")
        if name != None and name == table:
            reset = table_schema.get("reset")
            if reset != None and reset == "False":
                reset_is_false = False
                return False
    return True


def reset_tables_of_test_inputs(client, table_ddl_url, table_schemas, test_case):
    steps = test_case.get("steps")
    tables_recreated = []
    for step in steps:
        if "inputs" in step:
            inputs = step.get("inputs")
            for input in inputs:  # clean table data before each inputs walk through
                logger.debug(f"input in inputs = {input}")
                table = input.get("table_name")
                is_table_reset = None
                logger.debug(f"table of input in inputs = {table}")
                is_table_reset = find_table_reset_in_table_schemas(table, table_schemas)
                if (
                    is_table_reset != None and is_table_reset == False
                ) or table in tables_recreated:
                    pass
                else:
                    if table_exist(table_ddl_url, table):
                        res = client.execute(f"drop stream if exists {table}")
                        logger.debug(f"drop stream if exists {table} res = {res}")
                        drop_start_time = datetime.datetime.now()
                        logger.info(
                            f"drop stream if exists {table} is called successfully"
                        )
                        wait_count = 0
                        while table_exist(table_ddl_url, table):
                            time.sleep(0.01)
                            wait_count += 1
                        # wait_time = wait_count * 10
                        drop_complete_time = datetime.datetime.now()
                        time_spent = drop_complete_time - drop_start_time
                        time_spent_ms = time_spent.total_seconds() * 1000
                        global TABLE_DROP_RECORDS
                        TABLE_DROP_RECORDS.append(
                            {"table_name": {table}, "time_spent": time_spent_ms}
                        )
                        logger.info(f"table {table} is dropped, {time_spent_ms} spent.")

                    if table_schemas != None:
                        for table_schema in table_schemas:
                            name = table_schema.get("name")
                            if name == table and table_exist(table_ddl_url, table):
                                logger.debug(
                                    f"drop stream and re-create once case starts, table_ddl_url = {table_ddl_url}, table_schema = {table_schema}"
                                )
                                while table_exist(table_ddl_url, table):
                                    logger.debug(
                                        f"{name} not dropped succesfully yet, wait ..."
                                    )
                                    time.sleep(0.2)
                                logger.debug(
                                    f"drop stream and re-create once case starts, table {table} is dropped"
                                )
                                create_table_rest(table_ddl_url, table_schema)
                                while not table_exist(table_ddl_url, table):
                                    logger.debug(
                                        f"{name} not recreated successfully yet, wait ..."
                                    )
                                    time.sleep(0.2)
                                tables_recreated.append(name)
                            elif name == table and not table_exist(
                                table_ddl_url, table
                            ):
                                create_table_rest(table_ddl_url, table_schema)
                                while not table_exist(table_ddl_url, table):
                                    logger.debug(
                                        f"{name} not recreated successfully yet, wait ..."
                                    )
                                    time.sleep(0.2)
                                tables_recreated.append(name)
        if len(tables_recreated) > 0:
            logger.debug(f"tables: {tables_recreated} are dropted and recreated.")
    return tables_recreated


def test_case_collect(test_suite, tests_2_run, test_ids_set, proton_setting):
    test_suite_name = test_suite.get("test_suite_name")
    tests = test_suite.get("tests")
    tests_ids = []
    for test in tests:
        test_id = test.get("id")
        if test_id != None:
            tests_ids.append(test_id)
    logger.debug(
        f"test_suite_name = {test_suite_name}, len(tests) = {len(tests)}, tests_2_run = {tests_2_run}, test_ids_set={test_ids_set}"
    )
    test_run_list = []
    test_run_id_list = []
    if test_ids_set != None:
        test_ids_set_list = test_ids_set.split(",")
    # proton_ci_mode = "local" # for debug use.

    if (
        test_ids_set == None and tests_2_run == None
    ):  # if tests_2_run is not set, run all tests.
        test_run_list = tests
    elif test_ids_set == "all" and tests_2_run == None:
        test_run_list = tests
    elif test_ids_set != None and test_ids_set != "all":
        ids_2_run = []
        for each in test_ids_set_list:
            if each.isdigit() and int(each) in tests_ids:
                ids_2_run.append(int(each))
        for test in tests:
            id = test.get("id")
            if id in ids_2_run:
                test_run_list.append(test)
                test_run_id_list.append(id)

        logger.info(
            f"test_suite_name = {test_suite_name}, tests_run_id_list = {test_run_id_list}, {len(tests)} cases in total, {len(test_run_list)} cases collected"
        )

    else:  # if tests_2_run is set in test_suite_config, run the id list.
        logger.debug(f"tests_2_run is configured as {tests_2_run}")
        ids_2_run = tests_2_run.get("ids_2_run")
        tags_2_run = tests_2_run.get("tags_2_run")
        tags_2_skip_set = tests_2_run.get("tags_2_skip")
        tags_2_skip = []
        for key in tags_2_skip_set:
            if "default" in key:
                tags_2_skip += tags_2_skip_set[key]
            if proton_setting in key:
                tags_2_skip += tags_2_skip_set[proton_setting]
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

            test_run_list_len = len(test_run_list)

            # assert test_run_list_len != 0

            logger.info(
                f"test_suite_name = {test_suite_name}, tests_run_id_list = {test_run_id_list}, {len(tests)} cases in total, {test_run_list_len} cases to run in total"
            )
    return test_run_list


def test_suite_run(
    proton_setting,config, test_suite_run_ctl_queue, test_suite_result_done_queue, test_suite_set_dict
):
    logger = mp.get_logger()
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logging_level = test_suite_set_dict.get("logging_level")
    logger.addHandler(console_handler)
    logger.setLevel(logging_level)

    logger.debug(
        f"query_execute starts, logging_level = {logging_level}, logger.handlers = {logger.handlers}"
    )
    # run the test suite in a standlone process
    test_suite_name = test_suite_set_dict.get("test_suite_name")
    logger.info(f"test_suite: {test_suite_name} running starts......")
    test_suite = test_suite_set_dict.get("test_suite")
    query_results_queue = test_suite_set_dict.get("query_results_queue")
    # q_exec_client = test_suite_set_dict.get("query_exe_client")

    (
        query_conn,  # query_exe_parent_conn
        q_exec_client_conn,  # query_exe_child_conn
    ) = mp.Pipe(
        True
    )  # create the pipe for inter-process conn of each test_suite_run and query_execute pair, control path

    # query_conn = test_suite_set_dict.get("query_exe_parent_conn")
    # q_exec_client_conn = test_suite_set_dict.get("query_exe_child_conn")
    alive = test_suite_set_dict.get("alive")
    query_exe_client = mp.Process(
        target=query_execute,
        args=(
            config,
            q_exec_client_conn,
            query_results_queue,
            alive,
            logging_level,
        ),
    )  # Create query_exe_client process

    query_results_queue = test_suite_set_dict.get("query_results_queue")
    logger.debug(f"alive.value = {alive.value}")
    query_exe_client.start()  # start the query execute process
    logger.debug(f"query_exe_client: {query_exe_client} started.")

    rest_setting = config.get("rest_setting")
    proton_server = config.get("proton_server")
    proton_server_native_ports = config.get("proton_server_native_port")
    proton_server_native_ports = proton_server_native_ports.split(",")
    proton_server_native_port = proton_server_native_ports[
        0
    ]  # todo: assign proton_server and port at config for test_suite reset_tables_of_test_inputs

    test_ids_set = os.getenv("PROTON_TEST_IDS", None)

    table_ddl_url = rest_setting.get("table_ddl_url")

    test_run_list_len_total = 0
    test_sets = []  # test_set for collecting testing results of all test_suites
    if test_suite != None and len(test_suite) != 0:
        test_id_run = 0

        test_suite_config = test_suite.get("test_suite_config")
        if test_suite_config != None:
            table_schemas = test_suite_config.get("table_schemas")
        else:
            table_schemas = []
        tests = test_suite.get("tests")
        tests_2_run = test_suite_config.get("tests_2_run")
        test_run_list = []
        test_run_id_list = []
        logger.debug("test_case_collect is to be started......")
        test_run_list = test_case_collect(test_suite, tests_2_run, test_ids_set, proton_setting)
        test_run_list_len = len(test_run_list)
        if test_run_list_len == 0:
            logger.debug(
                f"test_suite_name = {test_suite_name}, test_run_list = {test_run_list}, 0 case collected, bypass."
            )
            pass
        else:
            try:
                client = Client(host=proton_server, port=proton_server_native_port)
                if test_suite_config != None:
                    logger.debug(f"test_suite_env_setup is to be started......")
                    tables_setup = test_suite_env_setup(
                        client, config, test_suite_config
                    )
                    logger.info(
                        f"test_suite_name = {test_suite_name}, tables_setup = {tables_setup} done."
                    )
                    logger.info(
                        f"test_suite_name = {test_suite_name}, len(test_run_list) = {len(test_run_list)} case collected."
                    )
                else:
                    logger.info(
                        f"test_suite_name = {test_suite_name}, no test_suite_config, bypass test_suite_env_setup"
                    )
                i = 0
                while i < len(test_run_list):
                    test_case = test_run_list[i]
                    statements_results = []
                    inputs_record = []
                    test_id = test_case.get("id")
                    test_name = test_case.get("name")
                    steps = test_case.get("steps")
                    # logger.debug(f"rockets_run: test_id = {test_id}, test_case = {test_case}, steps = {steps}")
                    expected_results = test_case.get("expected_results")
                    step_id = 0
                    auto_terminate_queries = []
                    # scan steps to find out tables used in inputs and truncate all the tables

                    tables_recreated = reset_tables_of_test_inputs(
                        client, table_ddl_url, table_schemas, test_case
                    )
                    logger.info(
                        f"tables: {tables_recreated} are dropted and recreated."
                    )

                    logger.info(
                        f"test_id_run = {test_id_run}, test_suite_name = {test_suite_name}, test_id = {test_id} starts......"
                    )

                    for step in steps:
                        statements_id = 0
                        inputs_id = 0

                        if "statements" in step:
                            step_statements = step.get("statements")
                            logger.debug(
                                f"test_suite_name = {test_suite_name}, step_statements = {step_statements}"
                            )
                            query_walk_through_res = query_walk_through(
                                step_statements, query_conn
                            )
                            statement_result_from_query_execute = query_walk_through_res
                            logger.debug(
                                f"query_walk_through_res = {query_walk_through_res}"
                            )

                            if (
                                statement_result_from_query_execute != None
                                and len(statement_result_from_query_execute) > 0
                            ):
                                for element in statement_result_from_query_execute:
                                    statements_results.append(element)

                            logger.info(
                                f"rockets_run: {test_id_run}, test_suite_name = {test_suite_name},  test_id = {test_id}, step{step_id}.statements{statements_id}, done..."
                            )

                            statements_id += 1
                        elif "inputs" in step:
                            inputs = step.get("inputs")
                            logger.info(
                                f"test_id_run = {test_id_run}, test_suite_name = {test_suite_name},  test_id = {test_id} inputs = {inputs}"
                            )

                            inputs_record = input_walk_through_rest(
                                rest_setting, inputs, table_schemas
                            )  # inputs walk through rest_client
                            logger.info(
                                f"test_id_run = {test_id_run}, test_suite_name = {test_suite_name},  test_id = {test_id} input_walk_through done"
                            )
                            # time.sleep(0.5) #wait for the data inputs done.
                        step_id += 1

                    query_conn.send("test_steps_done")
                    logger.debug("test_steps_done sent to query_execute")

                    message_recv = (
                        query_conn.recv()
                    )  # wait the query_execute to send "case_result_done" to indicate all the statements in pipe are consumed.

                    logger.debug(
                        f"rockets_run: mssage_recv from query_execute = {message_recv}"
                    )
                    assert message_recv == "case_result_done"

                    while (
                        not query_results_queue.empty()
                    ):  # collect all the query_results from queue after "case_result_done" received
                        time.sleep(0.2)
                        message_recv = query_results_queue.get()
                        logger.debug(
                            f"rockets_run: message_recv of query_results_queue.get() = {message_recv}"
                        )
                        query_results = json.loads(message_recv)
                        statements_results.append(query_results)

                    test_sets.append(
                        {
                            "test_suite_name": test_suite_name,
                            "test_id_run": test_id_run,
                            "test_id": test_id,
                            "test_name": test_name,
                            "steps": steps,
                            "expected_results": expected_results,
                            "statements_results": statements_results,
                        }
                    )
                    i += 1
                    test_id_run += 1

            except (BaseException) as error:
                logger.info(f"exception: {error}")

            finally:
                test_suite_run_ctl_queue.get()
                test_suite_run_ctl_queue.task_done()

            logger.info(
                f"test_suite_name = {test_suite_name} running ends, test_sets = {test_sets}......"
            )
        test_suite_result_summary = {
            "test_suite_name": test_suite_name,
            "test_run_list_len": test_run_list_len,
            "test_sets": test_sets,
        }

        test_suite_result_done_queue.put(test_suite_result_summary)
        test_suite_result_done_queue.join()

    TESTS_QUERY_RESULTS = test_sets
    query_conn.send("tear_down")
    message_recv = query_conn.recv()
    query_results_queue.close()
    query_conn.close()
    q_exec_client_conn.close()
    alive.value = False
    # q_exec_client.terminate()
    query_exe_client.join()
    del alive
    client.disconnect()
    logger.debug(f"TABLE_CREATE_RECORDS = {TABLE_CREATE_RECORDS}")
    logger.debug(f"TABLE_DROP_RECORDS = {TABLE_DROP_RECORDS}")
    count = 0
    time_spent_create = 0
    for item in TABLE_CREATE_RECORDS:
        time_spent_create = time_spent_create + item.get("time_spent")
        count += 1
    if count != 0:
        avg_time_spent_create = time_spent_create / count
    else:
        avg_time_spent_create = 0
    logger.info(
        f"table create {count} times, total time spent = {time_spent_create}ms, avg_time_spent_create = {avg_time_spent_create}"
    )
    count = 0
    time_spent_drop = 0
    for item in TABLE_DROP_RECORDS:
        time_spent_drop = time_spent_drop + item.get("time_spent")
        count += 1
    if count != 0:
        avg_time_spent_drop = time_spent_drop / count
    else:
        avg_time_spent_drop = 0
    logger.info(
        f"table drop {count} times, total time spent = {time_spent_drop}ms, avg_time_spent_create = { avg_time_spent_drop}"
    )

    return (test_run_list_len_total, test_sets)


# @pytest.fixture(scope="module")
def rockets_run(test_context):
    # todo: split tests.json to test_suite_config.json and tests.json
    root_logger = logging.getLogger()
    logger.info(
        f"rockets_run starts..., root_logger.level={root_logger.level}, logger.level={logger.level}"
    )
    if root_logger.level != None and root_logger.level == 20:
        logging_level = "INFO"
    else:
        logging_level = "DEBUG"
    docker_compose_file = test_context.get("docker_compose_file")
    proton_setting = test_context.get("proton_setting")
    config = test_context.get("config")
    proton_ci_mode = os.getenv("PROTON_CI_MODE", "Github")
    test_suites_selected_sets = None
    test_suites_selected_sets = test_context.get("test_suites_selected_sets")
    test_suite_run_ctl_queue = test_context.get("test_suite_run_ctl_queue")
    test_suite_result_done_queue = test_context.get("test_suite_result_done_queue")
    test_suite_query_reulst_queue_list = test_context.get(
        "test_suite_query_reulst_queue_list"
    )
    rest_setting = config.get("rest_setting")
    if test_suites_selected_sets != None and len(test_suites_selected_sets) != 0:
        env_setup_res = env_setup(rest_setting, docker_compose_file, proton_ci_mode)
        logger.info(f"rockets_run env_etup done, env_setup_res = {env_setup_res}")
    else:
        sys.exit(1)

    test_suite_runners = []
    test_sets = []
    test_suite_count = 1
    for test_suite_set_dict in test_suites_selected_sets:
        test_suite_name = test_suite_set_dict.get("test_suite_name")
        test_suite_run_ctl_queue.put("run a test suite")

        test_suite_runner = mp.Process(
            target=test_suite_run,
            args=(
                proton_setting,
                config,
                test_suite_run_ctl_queue,
                test_suite_result_done_queue,
                test_suite_set_dict,
            ),
        )
        # time.sleep(random.randint(1,10)) # start test_suite_run processes in a random time gap to avoid ddl operation in parallel to trigger 159
        time.sleep(test_suite_count * 2 + 1)
        test_suite_runner.start()
        test_suite_runners.append(
            {
                "test_suite_name": test_suite_name,
                "test_suite_runner": test_suite_runner,
            }
        )
        test_suite_count += 1

    try:
        test_suite_run_ctl_queue.join()
        test_suite_result_collect_done = 0
        test_run_list_len_total = 0
        while test_suite_result_collect_done < len(test_suites_selected_sets):
            test_suite_result_summary = test_suite_result_done_queue.get()
            test_suite_name_recvd = test_suite_result_summary.get("test_suite_name")
            test_run_list_len_recvd = test_suite_result_summary.get("test_run_list_len")
            test_sets_recvd = test_suite_result_summary.get("test_sets")

            logger.debug(
                f"test_suite: {test_suite_name_recvd}, test_suite_summary is received, len(test_sets_recvd)={len(test_sets_recvd)}, test_run_list_len_recvd = {test_run_list_len_recvd}"
            )
            test_run_list_len_total += test_run_list_len_recvd
            test_sets.extend(test_sets_recvd)
            logger.debug(
                f"test_suite: {test_suite_name} result received, len(test_sets) after test_sets.extend(test_sets_recvd) = {len(test_sets)}"
            )
            test_suite_result_done_queue.task_done()
            test_suite_result_collect_done += 1
            time.sleep(random.random())

    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")

    for test_suite_runner_dict in test_suite_runners:
        test_suite_runner_dict["test_suite_runner"].join()
    logger.debug(
        f"test_run_list_len_total = {test_run_list_len_total}, len(test_sets) = {len(test_sets)}"
    )
    return (test_run_list_len_total, test_sets)


if __name__ == "__main__":

    cur_file_path = os.path.dirname(os.path.abspath(__file__))
    cur_file_path_parent = os.path.dirname(cur_file_path)
    test_suite_path = None

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)

    if logger.level == 20:  # todo: get handling logger.leve gracefully
        logging_level = "INFO"
    else:
        logging_level = "DEBUG"

    #    logging_config_file = f"{cur_file_path}/logger.conf"
    #    if os.path.exists(logging_config_file):
    #        logger.basicConfig(
    #            format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    #        )  # todo: add log stuff
    #        logger.config.fileConfig(logging_config_file)  # need logger.conf
    #        logger = logger.getLogger("rockets")
    #    else:
    #        logger.info("no logger.conf exists under ../helper, no logger.")

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
        rockets_context = rockets_context(
            config_file, tests_file, docker_compose_file
        )  # need to have config env vars/config.json and test.json when run rockets.py as a test debug tooling.
        test_sets = rockets_run(rockets_context)
        # output the test_sets one by one
        logger.info("main: ouput test_sets......")
        for test_set in test_sets:
            test_set_json = json.dumps(test_set)
            logger.info(f"main: test_set from rockets_run: {test_set_json} \n\n")
    else:
        logger.info("No tests.json exists under test suite folder.")
