#!/usr/bin/python3
# _*_ coding: utf-8 _*_
import datetime, yaml, json, getopt, logging, logging.config, math, os, platform, random, requests, signal, subprocess, sys, threading, time, traceback, uuid
import multiprocessing as mp
from proton_driver import Client, errors
from timeplus import Stream, Environment
from helpers.event_util import (
    Event,
    EventRecord,
    TestEventTag,
    TestSuiteInfoTag,
    TestSuiteEventTag,
)
from enum import Enum, unique


cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

TABLE_CREATE_RECORDS = []
TABLE_DROP_RECORDS = []
VIEW_CREATE_RECORDS = []
QUERY_RUN_RECORDS = []

NONE_STREAM_NODE_FIRST = "none_stream_node_first"
SINGLE_STREAM_ONLY_NODE_FIRST = "single_stream_only_node_first"
STREAM_ONLY_NODE_FIRST = "stream_only_node_first"
VIEW_ONLY_NODE_FIRST = "view_only_node_first"
HOST_ALL_NODE_FIRST = "host_all_node_first"
HOST_NONE_NODE_FIRST = "host_none_node_first"

DEFAULT_TEST_SUITE_TIMEOUT = 900  # 1800 #seconds
DEFAULT_CASE_TIMEOUT = 60  # seconds, todo: case level timeout guardian
CASE_RETRY_UP_LIMIT = 5  # test suite case retry up limit, test_suite_run retry case only when failed case number less than this value
CASE_RETRY_TIMES = 3  # if retry again if failed case found in retry
TEST_SUITE_LAUNCH_INTERVAL = (
    10  # default value will be covered by the setting in config file
)

ACTS_IN_FINAL = (
    "kill",
    "drop_view",
    "drop_stream",
)  # acts to be executed after the statement is done in finally clause
ACTS_IN_QUERY_RUN = "exist"  # scts to be executed right after query is run. only table query with 'create' in is supported so far

RUNNING_STATE = "running"  # statement /input_batch state
DONE_STATE = "done"
INIT_STATE = "init"

# alive = mp.Value('b', True)
TIME_STR_FORMAT = "%y/%m/%d, %H:%M:%S"


# TestException and errors, steal some codes from proton_driver.errors
class ErrorCodes:
    STREAM_CREATE_FAILED = 100
    QUERY_ID_NOT_EXIST = 101
    DROP_STREAM_ERROR = 102
    DROP_STREAM_REST_ERROR = 103
    INPUT_BATCH_REST_ERROR = 104
    QUERY_EXIST_CHECK_REST_FAILED = 105
    QUERY_EXIST_CHECK_NATIVE_FAILED = 106
    QUERY_DEPENDS_ON_FAILED_TO_START = 201
    QUERY_END_TIMER_DEPENDS_ON_NOT_EXIST = 202
    STREAM_DEPENDS_ON_NOT_EXIST = 203
    QUERY_INPUT_DEPENDS_ON_NOT_EXIST = 204
    QUERY_EXISTS_CLUSTER_ERROR = 205
    QUERY_CRASH_ERROR = 301
    QUERY_FATAL_ERROR = 302
    INPUT_ERROR = 303
    RESET_STREAMS_OF_INPUTS_ERROR = 304
    NO_CONFIG_EXIST = 401
    ENV_HEALTH_CHECK_FAILED = 402
    TEST_SUITE_ENV_SETUP_ERROR = 403
    TEST_CASE_COLLECTION_ERROR = 404
    TEST_SUITE_TIMEOUT_ERROR = 405
    TEST_CASE_INPUT_TABLE_NAME_NOT_FOUND = 406


class Error(Exception):
    code = None

    def __init__(self, message=None):
        self.message = message
        super(Error, self).__init__(message)

    def __str__(self):
        message = " " + self.message if self.message is not None else ""
        return "Code: {}.{}".format(self.code, message)


class TestException(Error):
    def __init__(self, message, code=None, nested=None):
        self.message = message
        self.code = code
        self.nested = nested
        super(TestException, self).__init__(message)

    def __str__(self):
        nested = "\nNested: {}".format(self.nested) if self.nested else ""
        return "Code: {}.{}\n{}".format(self.code, nested, self.message)


@unique
class QueryClientType(Enum):
    PyClient = "python"
    REST = "rest"
    EXEC = "exec"


def timeout_flag(
    timeout_hit_event, hit_msg, hit_context_info
):  # timeout_hit_event is a threading.Event, todo: use signal.signal() to register a signal handler and signal.alarm(timeout)
    try:
        timeout_hit_event.set()

        print(f"{str(datetime.datetime.now())}, {hit_context_info}, {hit_msg}")
        return
    except BaseException as error:
        traceback.print_exc()
        print(
            f"{str(datetime.datetime.now())}, {hit_context_info}, timeout_flag exception, error = {error}"
        )


def scan_tests_file_path(tests_file_path, proton_setting):
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
        test_suite_settings_2_run = None
        if (
            file_name.endswith(".json")
            or file_name.endswith(".yaml")
            or file_name.endswith(".yml")
        ):
            file_abs_path = f"{tests_file_path}/{file_name}"
            logger.debug(f"file_abs_path = {file_abs_path}")
            with open(file_abs_path) as test_suite_file:
                if file_name.endswith(".json"):
                    test_suite = json.load(test_suite_file, strict=False)
                else:
                    test_suite = yaml.safe_load(test_suite_file)
                logger.debug(
                    f"test_suite_file = {test_suite_file}, was loaded successfully."
                )
                test_suite_name = test_suite.get("test_suite_name")
                test_suite_tag = test_suite.get("tag")
                test_suite_config = test_suite.get("test_suite_config")
                if test_suite_config is not None:
                    test_suite_settings_2_run = test_suite_config.get("settings_to_run")
                else:
                    test_suite_settings_2_run = None
                if test_suite_name == None or test_suite_tag == "skip":
                    logger.debug(
                        f"test_suite_name is vacant or test_suite_tag == skip and ignore this json file"
                    )
                    pass
                else:
                    logger.debug(
                        f"check if test_sute_name = {test_suite_name}, test_suite_settings_2_run = {test_suite_settings_2_run}, in test_suites_set_list = {test_suites_set_list}"
                    )
                    if test_suite_settings_2_run is None or (
                        test_suite_settings_2_run is not None
                        and proton_setting in test_suite_settings_2_run
                    ):
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
    logger.info(
        f"proton_setting = {proton_setting}, test_suite_settings_2_run = {test_suite_settings_2_run}, test_suite_names_selected = {test_suite_names_selected}"
    )
    return {
        "test_suite_names_selected": test_suite_names_selected,
        "test_suites_selected": test_suites_selected,
        "PROTON_TEST_SUITES": test_suites_set_env,
    }


def rockets_context(config_file, tests_file_path, docker_compose_file):
    test_suites = []
    test_suite_names_selected = []
    test_suites_selected = []
    test_suite_set_dict = {}
    test_suites_selected_sets = []  # a list of tuple of (test_suite, test_result_queue)
    test_suite_query_reulst_queue_list = (
        []
    )  # a list of map of test_sutie_name and test_suite_query_result_queue
    proton_ci_mode = os.getenv("PROTON_CI_MODE", "Github")
    proton_setting = os.getenv("PROTON_SETTING", "default")
    test_retry = os.getenv("TEST_RETRY", "True")

    test_suite_timeout = os.getenv(
        "TEST_SUITE_TIMEOUT", DEFAULT_TEST_SUITE_TIMEOUT
    )  # get TEST_SUITE_TIMEOUT env var which is set by ci_runner.py, if no env var get, 20mins for test suite execution timeout, this could be set in tests.json too.
    test_case_timeout = os.getenv("TEST_CASE_TIMEOUT", DEFAULT_CASE_TIMEOUT)
    proton_cluster_query_route_mode = os.getenv(
        "PROTON_CLUSTER_QUERY_ROUTE_MODE", "default"
    )
    proton_cluster_query_node = os.getenv("PROTON_CLUSTER_QUERY_NODE", "default")
    proton_create_stream_shards = os.getenv("PROTON_CREATE_STREAM_SHARDS")
    proton_create_stream_replicas = os.getenv("PROTON_CREATE_STREAM_REPLICAS")
    proton_sql_settings = os.getenv("SQL_SETTINGS")

    root_logger = logging.getLogger()
    logger.info(f"rockets_run starts..., root_logger.level={root_logger.level}")
    if root_logger.level != None and root_logger.level == 20:
        logging_level = "INFO"
    elif root_logger.level != None and root_logger.level == 10:
        logging_level = "DEBUG"
    elif root_logger.level is not None and root_logger.level == 30:
        logging_level = "WARNING"
    elif root_logger.level is not None and root_logger.level == 40:
        logging_level = "ERROR"
    elif root_logger.level is not None and root_logger.level == 50:
        logging_level = "CRITICAL"
    else:
        logging_level = "INFO"
    configs = {}
    with open(config_file) as f:
        if config_file.endswith(".json"):
            configs = json.load(f)
        elif config_file.endswith(".yaml") or config_file.endswith(".yml"):
            configs = yaml.safe_load(f)
    timeplus_event_stream = configs.get(
        "timeplus_event_stream"
    )  # todo: distribute global configs into configs
    timeplus_event_version = configs.get("timeplus_event_version")
    config = configs.get(proton_setting)
    logger.debug(f"setting = {proton_setting},config = {config}")

    if config == None:
        error_msg = f"NO_CONFIG_EXIST, no config found for setting = {proton_setting}"
        logger.error(error_msg)
        no_config_exist_exception = TestException(error_msg, ErrorCodes.NO_CONFIG_EXIST)
        raise no_config_exist_exception
    test_event_tag = os.environ.get("TIMEPLUS_TEST_EVENT_TAG", None)
    config["proton_ci_mode"] = proton_ci_mode
    config["proton_setting"] = proton_setting  # put proton_setting into config
    config["test_retry"] = test_retry  # put test_retry into config
    config["test_suite_timeout"] = int(
        test_suite_timeout
    )  # put test_suite_timeout into config
    config["test_case_timeout"] = int(test_case_timeout)
    config["proton_create_stream_shards"] = proton_create_stream_shards
    config["proton_create_stream_replicas"] = proton_create_stream_replicas
    if test_event_tag is not None:
        test_event_tag = json.loads(test_event_tag)
        config["test_event_tag"] = test_event_tag
    config["timeplus_event_stream"] = timeplus_event_stream
    config["timeplus_event_version"] = timeplus_event_version
    if proton_sql_settings is not None:
        proton_sql_settings_dict = json.loads(proton_sql_settings)
        config["proton_sql_settings"] = proton_sql_settings_dict
    if (
        "cluster" in proton_setting
    ):  # if running under cluster mode and proton_cluster_query_mode, take proton_cluster_query_route_mode as part of config
        config[
            "proton_cluster_query_node"
        ] = proton_cluster_query_node  # put proton_cluster_query_node into config
        config["proton_cluster_query_route_mode"] = proton_cluster_query_route_mode

    res_scan_tests_file_path = scan_tests_file_path(tests_file_path, proton_setting)
    test_suite_names_selected = res_scan_tests_file_path.get(
        "test_suite_names_selected"
    )
    test_suites_selected = res_scan_tests_file_path.get("test_suites_selected", {})
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
    rockets_context = {
        # "proton_setting": proton_setting,  # todo: refactor, remove this, the proton_setting is only readed from config
        "config": config,
        "test_suite_run_ctl_queue": test_suite_run_ctl_queue,
        "test_suite_result_done_queue": test_suite_result_done_queue,
        "test_suite_query_result_queue_list": test_suite_query_reulst_queue_list,
        "test_suite_names_selected": test_suite_names_selected,
        "test_suites_selected_sets": test_suites_selected_sets,
        "docker_compose_file": docker_compose_file,
    }
    ROCKETS_CONTEXT = rockets_context
    # logger.debug(f"config = {config}")
    return rockets_context


def env_health_check(health_check_url):
    try:
        logger.debug(f"env_health_check: health_check_url = {health_check_url}")
        res = requests.get(health_check_url)
        if res.status_code == 200:
            return True
        else:
            print(f"requests.get({health_check_url}, status_code = {res.status_code}) ")
            return False
    except BaseException as error:
        print(f"requests.get({health_check_url} exception: {error}")
        command = "docker ps"
        res = subprocess.run(
            command,
            shell=True,
            encoding="utf-8",
            timeout=5,
            capture_output=True,
        )
        print(f"docker ps result: #######\n {res}")
        command = "docker logs redpanda-1"
        res = subprocess.run(
            command,
            shell=True,
            encoding="utf-8",
            timeout=5,
            capture_output=True,
        )
        print(f"docker logs redpanda-1 result: #######\n {res}")
        return False


def env_check(
    rest_settings,
    proton_ci_mode="local",
):
    ci_mode = proton_ci_mode
    logger.info(f"env_check: ci_mode = {ci_mode}")
    logger.debug(f"env_check: rest_settings = {rest_settings}")
    env_health_check_res_list = []
    for rest_setting in rest_settings:
        health_url = rest_setting.get("health_check_url")
        print(f"env_check: health_url = {health_url}")
        tables_cleaned = []
        env_health_check_res = env_health_check(health_url)
        print(f"env_check: env_health_check_res: {env_health_check_res}")
        retry = 30
        while env_health_check_res == False and retry > 0:
            time.sleep(4)
            env_health_check_res = env_health_check(health_url)
            print(
                f"env_health_check retry, env_check: health_url = {health_url}, retry = {retry}"
            )
            retry -= 1
        if env_health_check_res == False:
            error_msg = f"ENV_HEALTH_CHECK_FAILED, env_health_check_res = {env_health_check_res}, health_url = {health_url}"
            logger.error(error_msg)
            env_health_check_exception = TestException(
                error_msg, ErrorCodes.ENV_HEALTH_CHECK_FAILED
            )
            raise env_health_check_exception
        env_health_check_res_list.append(env_health_check_res)
    if ci_mode == "github":
        time.sleep(
            10
        )  # health check rest is not accurate, wait after docker compsoe up under github mode, remove later when it's fixed.
    return {
        "env_health_check_res_list": env_health_check_res_list,
    }


def get_top():
    try:
        os_info = platform.platform()
        if "Linux" in os_info:
            command = "top -bn1 |head -10"
        else:
            return f"os is not linux, no top info collected."  # todo: support other OS later.
        res = subprocess.run(
            command,
            shell=True,
            encoding="utf-8",
            timeout=5,
            capture_output=True,
        )
        return f"top result: #######\n {res}"
    except BaseException as error:
        logger.debug(f"get_top exception: error = error")
        traceback.print_exc()
        return "get_top error, no top info collected"


def run_test_suites(
    config,
    test_suite_run_ctl_queue,
    test_suites_selected_sets,
    test_suite_result_done_queue,
):
    test_summary = {}
    module_summary = {}
    case_results = {}
    test_report = {
        "test_summary": test_summary,
        "module_summary": module_summary,
        "case_results": case_results,
    }
    test_suite_runners = []
    test_sets = []
    test_run_list_total = []
    failed_cases = []  # record the failed cases for retry
    test_suite_count = 1
    proton_setting = config.get("proton_setting")
    logger.debug(
        f"proton_setting = {proton_setting}, total {len(test_suites_selected_sets)} test suties to be launched."
    )
    test_suite_names = []
    multi_protons = config.get("multi_protons")
    top_info = ""
    test_suite_launch_interval = TEST_SUITE_LAUNCH_INTERVAL  # 10 seconds by default
    sleep_time = test_suite_launch_interval
    proton_configs = []
    query_states_list = []
    if (
        multi_protons == True
    ):  # if multi_protons is True, there are multiple settings for allocating the test suites on configs
        proton_configs = list(config["settings"].values())
        for (
            proton_config
        ) in (
            proton_configs
        ):  # todo: auto read attributes from config and distribute the config to each setting
            proton_config["proton_ci_mode"] = config["proton_ci_mode"]
            proton_config["proton_setting"] = config["proton_setting"]
            proton_config["test_suite_timeout"] = config["test_suite_timeout"]
            proton_config["test_retry"] = config["test_retry"]
            proton_config["test_case_timeout"] = config["test_case_timeout"]
            test_event_tag = config.get("test_event_tag")
            if test_event_tag is not None:
                proton_config["test_event_tag"] = test_event_tag
            proton_sql_settings = config.get("proton_sql_settings")
            if proton_sql_settings is not None:
                proton_config["proton_sql_settings"] = proton_sql_settings
            proton_config["timeplus_event_stream"] = config["timeplus_event_stream"]
            proton_config["timeplus_event_version"] = config["timeplus_event_version"]
            proton_config["proton_create_stream_shards"] = config[
                "proton_create_stream_shards"
            ]
            proton_config["proton_create_stream_replicas"] = config[
                "proton_create_stream_replicas"
            ]
            proton_cluster_query_node = config.get("proton_cluster_query_node")
            if (
                proton_cluster_query_node is not None
            ):  # todo: support and optimize handle multi cluster settings in one env later.
                proton_config["proton_cluster_query_node"] = config[
                    "proton_cluster_query_node"
                ]
            proton_cluster_query_route_mode = config.get(
                "proton_cluster_query_route_mode"
            )
            if proton_cluster_query_route_mode is not None:
                proton_config[
                    "proton_cluster_query_route_mode"
                ] = proton_cluster_query_route_mode
            ci_runner_params = config.get("ci_runner_params")
            if ci_runner_params is not None and len(ci_runner_params) > 0:
                for param in ci_runner_params:
                    for key, value in param.items():
                        if key == "test_suite_launch_interval":
                            test_suite_launch_interval = int(
                                value
                            )  # get the interval for lanuch concurrent test_suite running, if interval is too small in kafka mode, Code: 159 error will happen and lots case would be failed.
            proton_config["test_suite_launch_interval"] = test_suite_launch_interval
    else:
        ci_runner_params = config.get("ci_runner_params")
        if ci_runner_params is not None and len(ci_runner_params) > 0:
            for param in ci_runner_params:
                for key, value in param.items():
                    if key == "test_suite_launch_interval":
                        test_suite_launch_interval = int(
                            value
                        )  # get the interval for lanuch concurrent test_suite running, if interval is too small in kafka mode, Code: 159 error will happen and lots case would be failed.
        config["test_suite_launch_interval"] = test_suite_launch_interval
    i = 0  # count for the proton_settings in config when multi_protons == True
    j = 0  # counter for test_suites launch
    for test_suite_set_dict in test_suites_selected_sets:
        test_suite_name = test_suite_set_dict.get("test_suite_name")
        test_suite_names.append(test_suite_name)
        test_suite_run_ctl_queue.put("run a test suite")
        proton_server_container_name = ""
        if (
            multi_protons == True
        ):  # if multi_protons is True, there are multiple settings for allocating the test suites on configs
            proton_config = proton_configs[i]
            proton_server_container_name = proton_config["proton_server_container_name"]
            i += 1
            if i == len(proton_configs):
                i = 0
        else:
            proton_config = config
            proton_configs = [proton_config]
            proton_server_container_name = proton_config["proton_server_container_name"]
        test_suite_launch_interval = int(proton_config["test_suite_launch_interval"])
        to_start_at = proton_config.get("to_start_at")
        if to_start_at is None:
            if j // len(proton_configs) > 0:
                to_start_at = datetime.datetime.now() + datetime.timedelta(
                    seconds=test_suite_launch_interval
                )
                to_start_at_str = to_start_at.strftime(TIME_STR_FORMAT)
            else:
                to_start_at_str = None
        else:
            to_start_at = datetime.datetime.strptime(to_start_at, TIME_STR_FORMAT)
            to_start_at = to_start_at + datetime.timedelta(
                seconds=test_suite_launch_interval
            )
            to_start_at_str = to_start_at.strftime(TIME_STR_FORMAT)
        proton_config["to_start_at"] = to_start_at_str
        mp_mgr = mp.Manager()
        query_states_dict = (
            mp_mgr.dict()
        )  # to store the states of stream query run as a standlone query, query_states_dict: {test_id:{query_id:state, ...}, ...}
        query_states_dict["test_suite_name"] = test_suite_name
        query_states_dict["proton_setting"] = proton_setting
        query_states_dict["proton_server_container_name"] = proton_server_container_name
        logger.debug(
            f"test_suite_name = {test_suite_name}, query_states_dict = {query_states_dict}"
        )
        test_suite_obj = TestSuite(
            proton_config,
            test_suite_run_ctl_queue,
            test_suite_result_done_queue,
            test_suite_set_dict,
            query_states_dict,
        )
        test_suite_runner = mp.Process(
            target=test_suite_obj.run,
            args=(),
        )
        query_states_list.append(query_states_dict)
        test_suite_count += 1
        j += 1
        test_suite_runner.start()
        top_info = get_top()
        logging.debug(
            f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name} is launched on proton_server_container_name = {proton_server_container_name}, query_states_dict = {query_states_dict}, top_info = {top_info}."
        )
        test_suite_runners.append(
            {
                "test_suite_name": test_suite_name,
                "test_suite_runner": test_suite_runner,
                "proton_setting": proton_setting,
                "proton_server_container_name": proton_server_container_name,
                "status": "",  # status: running, done
            }
        )
    logger.debug(
        f"proton_setting = {proton_setting}, total {len(test_suites_selected_sets)} test suties are launched: {test_suite_names}"
    )
    test_run_list_len_total = 0
    test_suite_result_summary_list = []
    try:
        test_suite_run_ctl_queue.join()
        test_suite_result_collect_done = 0
        logger.debug(
            f"test_suite_result_collect_done = {test_suite_result_collect_done},len(test_suites_selected_sets) = {len(test_suites_selected_sets)} "
        )
        while test_suite_result_collect_done < len(test_suites_selected_sets):
            test_suite_result_summary = test_suite_result_done_queue.get()
            test_suite_result = True
            test_suite_name_recvd = test_suite_result_summary.get("test_suite_name")
            test_run_list_len_recvd = test_suite_result_summary.get("test_run_list_len")
            test_run_list_recvd = test_suite_result_summary.get("test_run_list")
            test_sets_recvd = test_suite_result_summary.get("test_sets")

            logger.debug(
                f"test_suite: {test_suite_name_recvd}, test_suite_summary is received, len(test_sets_recvd)={len(test_sets_recvd)}, test_run_list_len_recvd = {test_run_list_len_recvd}"
            )
            test_run_list_len_total += test_run_list_len_recvd
            test_run_list_total.append(test_run_list_recvd)
            for test_set in test_sets_recvd:
                test_result = TestSuite.case_result_check(
                    test_set
                )  # as soon as a test_suite_result_summary is received, check the test result and set the test_result field of each case
                test_set["test_result"] = test_result
                if not test_result:
                    failed_case = {
                        "proton_setting": proton_setting,
                        "test_suite_name": test_set["test_suite_name"],
                        "test_id": test_set["test_id"],
                        "test_name": test_set["test_id"],
                        "status": test_set["status"],
                        "test_result": test_set["test_result"],
                    }
                    failed_cases.append(failed_case)
                test_suite_result = test_suite_result & test_result
            test_suite_result_summary[
                "test_suite_result"
            ] = test_suite_result  # update the test_suite_result field of the test_suite_result_summary based on the case result check
            test_suite_result_summary_list.append(test_suite_result_summary)
            test_sets.extend(test_sets_recvd)
            test_suite_result_done_queue.task_done()
            test_suite_result_collect_done += 1
            time.sleep(random.random())
            test_sets_recvd_len = len(test_sets_recvd)
            logger.info(f"test_suite_name_recvd: {test_suite_name_recvd}")
            logger.debug(
                f"test_suite_result_collect_done = {test_suite_result_collect_done},len(test_suites_selected_sets) = {len(test_suites_selected_sets)}"
            )
            if test_sets_recvd_len != test_run_list_len_recvd:
                print(
                    f"test_suite_name_recvd = {test_suite_name_recvd}, test run list length mismatch with test sets,test_sets_recvd_len = {test_sets_recvd_len}, test_run_list_len_recvd = {test_run_list_len_recvd}, test_run_list_recvd = {test_run_list_recvd}, test_sets_recvd = {test_sets_recvd}"
                )
    except BaseException as error:
        logger.error(f"exception, error = {error}")
        traceback.print_exc()
    all_run_done = False
    while not all_run_done:
        print(f"\nTest_suite running status:")
        for test_suite_runner_dict in test_suite_runners:
            proc = test_suite_runner_dict["test_suite_runner"]
            exitcode = proc.exitcode
            test_suite_runner_dict["exit_code"] = exitcode
            if exitcode is None:
                test_suite_runner_dict["status"] = "running"
            else:
                test_suite_runner_dict["status"] = "done"
            print(
                f"test_suite_name = {test_suite_runner_dict['test_suite_name']}, proton_setting = {test_suite_runner_dict['proton_setting']}, proton_server_container_name = {test_suite_runner_dict['proton_server_container_name']}, running status = {test_suite_runner_dict['status']}, exitcode = {exitcode}"
            )
        for test_suite_runner_dict in test_suite_runners:
            if test_suite_runner_dict["status"] == "running":
                break
            all_run_done = True
        if not all_run_done:
            time.sleep(30)  # wait for 30 seconds before checking again
        else:
            for test_suite_runner_dict in test_suite_runners:
                print(
                    f"test_suite_name = {test_suite_runner_dict['test_suite_name']}, proton_setting = {test_suite_runner_dict['proton_setting']}, proton_server_container_name = {test_suite_runner_dict['proton_server_container_name']}, running status = {test_suite_runner_dict['status']}, exitcode = {test_suite_runner_dict['exit_code']}"
                )
    for test_suite_runner_dict in test_suite_runners:
        test_suite_runner_dict["test_suite_runner"].terminate()
    for test_suite_runner_dict in test_suite_runners:
        test_suite_runner_dict["test_suite_runner"].join()
    print(f"\nproton_setting = {proton_setting}, Test_Suites_Running_Statistics:\n")
    for test_suite_summary in test_suite_result_summary_list:
        test_suite_name = test_suite_summary.get("test_suite_name")
        test_suite_run_status = test_suite_summary.get("test_suite_run_status")
        test_set_list = test_suite_summary.get("test_sets")
        test_suite_run_duration = test_suite_summary.get("test_suite_run_duration")
        test_suite_case_run_duration = test_suite_summary.get(
            "test_suite_case_run_duration"
        )
        test_run_list_len = test_suite_summary.get("test_run_list_len")
        test_suite_passed_total = (test_suite_summary.get("test_suite_passed_total"),)
        proton_server_container_name = test_suite_summary.get(
            "proton_server_container_name"
        )
        print(
            f"proton_settings = {proton_setting}, test_suite_name = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, test_run_list_len = {test_run_list_len}, test_suite_passed_total = {test_suite_passed_total}, test_suite_case_run_duration = {test_suite_case_run_duration} seconds,test_suite_run_duration = {test_suite_run_duration} seconds "
        )
        for test_set in test_set_list:
            print(
                f'test_id = {test_set["test_id"]}, status = {test_set["status"]}, case_retried = {test_set["case_retried"]}, result = {test_set["test_result"]}, test_case_duration = {test_set["test_case_duration"]} seconds'
            )
    return (test_run_list_len_total, test_run_list_total, test_sets)


def rockets_run(test_context):
    # todo: split tests.json to test_suite_config.json and tests.json
    # root_logger = logging.getLogger()
    logger.info(f"rockets_run starts..., logger.level={logger.level}")
    if logger.level != None and logger.level == 20:
        logging_level = "INFO"
    else:
        logging_level = "DEBUG"
    docker_compose_file = test_context.get("docker_compose_file")
    config = test_context.get("config")
    proton_setting = config.get("proton_setting")
    proton_ci_mode = config.get("proton_ci_mode")
    test_suites_selected_sets = None
    test_suites_selected_sets = test_context.get("test_suites_selected_sets")
    test_suite_run_ctl_queue = test_context.get("test_suite_run_ctl_queue")
    test_suite_result_done_queue = test_context.get("test_suite_result_done_queue")
    test_suite_query_reulst_queue_list = test_context.get(
        "test_suite_query_reulst_queue_list"
    )
    rest_settings = []
    multi_protons = config.get("multi_protons")
    if multi_protons == True:
        for key in config["settings"]:
            rest_setting = config["settings"][key].get("rest_setting")
            rest_settings.append(rest_setting)
    else:
        rest_setting = config.get("rest_setting")
        rest_settings.append(rest_setting)
    if test_suites_selected_sets != None and len(test_suites_selected_sets) != 0:
        env_check_res = env_check(rest_settings, proton_ci_mode)
        logger.info(f"rockets_run env_check done, env_check_res = {env_check_res}")
    else:
        test_suites_set_env = os.getenv("PROTON_TEST_SUITES", None)
        print(
            f"######\n Wrong Test Suite Name \nci_runner.py --test_suite={test_suites_set_env}, test suite name {test_suites_set_env} is not found in any test suite json file! \n######\n"
        )
        sys.exit(1)
    test_run_list_len_total, test_run_list_total, test_sets = run_test_suites(
        config,
        test_suite_run_ctl_queue,
        test_suites_selected_sets,
        test_suite_result_done_queue,
    )
    return (test_run_list_len_total, test_run_list_total, test_sets)


class QueryClient(object):
    def __init__(self, type, **query_client_context):
        self._type = type
        self._query_client_context = query_client_context

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = type

    @property
    def query_client_context(self):
        return self._query_client_context

    @query_client_context.setter
    def query_client_context(self, query_client_context):
        self._query_client_context = query_client_context

    def run(self):
        pass

    @classmethod
    def query_state_check(cls, query_states_dict, state_4_check, test_id, query_id):
        try:
            if query_states_dict is not None:
                query_state = query_states_dict[str(test_id)].get(str(query_id))
                retry = 100
                while query_state != state_4_check and retry > 0:
                    time.sleep(0.1)
                    query_state = query_states_dict[str(test_id)].get(str(query_id))
                    retry -= 1
                if retry <= 0:
                    return False
                else:
                    return True
        except BaseException as error:
            logger.error(
                f"Query State Check, test_id = {test_id}, query_id = {query_id}, state_4_check = {state_4_check}, query_states_dict = {query_states_dict}, error: {error}"
            )
            traceback.print_exc()
            return False


class QueryClientPy(QueryClient):
    def __init__(self, type=QueryClientType.PyClient, **query_client_context):
        super().__init__(type, **query_client_context)

    @classmethod
    def scan_statement_and_act(
        cls, statement, act, pyclient, config=None
    ):  # scan batch inputs or query statement for kill keywords and execute kill query
        logger = mp.get_logger()
        try:
            act_target = statement.get(act)
            act_target_list = None
            if act_target is not None:
                act_target = str(act_target)
                act_target_list = act_target.split(",")
                act_wait_key = act + "_" + "wait"
                act_wait = statement.get(act_wait_key)
                if act_wait is not None:
                    time.sleep(int(act_wait))
                res = False
                if act_target_list is not None and len(act_target_list) > 0:
                    if act == "exist" and config:
                        rest_setting = config.get("rest_setting")
                        table_ddl_url = rest_setting.get("table_ddl_url")
                        res = QueryClientRest.depends_on_stream_exist(
                            table_ddl_url, act_target_list
                        )
                    else:
                        for item in act_target_list:
                            if act == "kill":
                                res = cls.kill_query(pyclient, item)
                            elif act == "drop_view":
                                res = cls.drop_if_exist_py(pyclient, "view", item)
                                time.sleep(1)
                            elif act == "drop_stream":
                                res = cls.drop_if_exist_py(pyclient, "stream", item)
                                time.sleep(1)
                    if not res:
                        logger.debug(
                            f"ACT {act} FAILED: act_list = {act_target_list}, res = {res}"
                        )
                return res
            return True
        except BaseException as error:
            if act == "exist":
                error_msg = f"CREATE STREAM/VIEW FATAL exception: statement = {statement}, exist checking after creating failed"
                stream_create_failed_exception = TestException(
                    error_msg, ErrorCodes.STREAM_CREATE_FAILED
                )
                raise stream_create_failed_exception
            else:
                return False

    @classmethod
    def table_exist_py(cls, pyclient, table_name):
        logger = mp.get_logger()
        sql_2_run = f"show streams where name = '{table_name}'"
        try:
            res = pyclient.execute(sql_2_run)
            for element in res:
                if table_name in element:
                    return True
            return False
        except BaseException as error:
            logger.error(f"exception, error = {error}")
            traceback.print_exc()
            return False

    @classmethod
    def query_id_exists_py(cls, py_client, query_id, query_exist_check_sql=None):
        logger = mp.get_logger()
        query_id = str(query_id)
        if query_exist_check_sql == None:
            query_exist_check_sql = (
                f"select query_id from system.processes where query_id = '{query_id}'"
            )
        try:
            res_check_query_id = py_client.execute(query_exist_check_sql)
            if res_check_query_id != None and isinstance(res_check_query_id, list):
                for element in res_check_query_id:
                    logger.debug(f"element = {element}, query_id = {query_id}")
                    if query_id in element:
                        return True
                return False
            else:
                logger.debug(f"query_id_list is None or not a list")
                return False
        except BaseException as error:
            error_string = f"query_id_exists_py, exception, Error = {error}"
            logger.error(f"query_id_exists_py, exception, Error = {error}")
            traceback.print_exc()
            if isinstance(error, errors.Error):
                error_msg = f"QUERY_ID_NOT_EXIST, Error = {error}"
                query_id_not_exist_exception = TestException(
                    error_msg, ErrorCodes.QUERY_ID_NOT_EXIST
                )
                raise query_id_not_exist_exception

        return False

    @classmethod
    def create_view_if_not_exit_py(cls, client, table_schema):
        table_type = table_schema.get("type")
        table_name = table_schema.get("name")
        if not QueryClientPy.table_exist_py(client, table_name):
            if table_type == "view":
                sql_2_run = table_schema.get("create_sql")
                if sql_2_run != None:
                    logger.debug(f"create_view_pyclient: sql_2_run = {sql_2_run}")
                else:
                    sql_2_run = f"drop view {table_name}"
                res_drop = client.execute(sql_2_run)
                logger.debug(f"create_view_pyclient: executed")
                retry = 100
                while retry < 100 and QueryClientPy.table_exist_py(client, table_name):
                    time.sleep(0.05)
                    retry -= 1
                if not QueryClientPy.table_exist_py(client, table_name):
                    logger.debug(f"create view {table_name} failed.")
                    return False
                else:
                    logger.debug(f"create view {table_name} success.")
                    return True
        else:
            logger.debug(f"{table_name} exist, bapass create view")
            return None

    @classmethod
    def drop_if_exist_py(cls, client, type, table_name):
        try:
            sql_2_run = None
            if QueryClientPy.table_exist_py(client, table_name):
                if str.upper(type) == "VIEW":
                    sql_2_run = f"drop view if exists {table_name}"
                elif str.upper(type) == "STREAM":
                    sql_2_run = f"drop stream if exists {table_name} settings enable_dependency_check = false"
                if sql_2_run is not None:
                    res_drop = client.execute(sql_2_run)
                    logger.debug(
                        f"drop {type} {table_name} is executed, res_drop = {res_drop}"
                    )
                retry = 100
                while retry > 0 and QueryClientPy.table_exist_py(client, table_name):
                    time.sleep(0.2)
                    retry -= 1
                if QueryClientPy.table_exist_py(client, table_name):
                    logger.debug(
                        f"drop {type} {table_name} is failed, QueryClientPy.table_exist_py({table_name}) = True"
                    )
                    return False
                else:
                    logger.debug(
                        f"drop {type} {table_name} is succesfully, QueryClientPy.table_exist_py({table_name}) = False"
                    )
                    return True
            else:
                logger.debug(f"{type} {table_name} does not exist, bypass drop")
                return True
        except BaseException as error:
            error_msg = f"DROP_STREAM_ERROR, error = {error}"
            logger.error(error_msg)
            drop_stream_exception = TestException(
                error_msg, ErrorCodes.DROP_STREAM_ERROR
            )
            traceback.print_exc()
            raise drop_stream_exception

    @classmethod
    def kill_query(cls, proton_client, query_2_kill, logging_level="INFO"):
        logger = mp.get_logger()
        logger.debug(f"kill_query starts, query_2_kill = {query_2_kill}")
        try:
            kill_sql = f"kill query where query_id = '{query_2_kill}'"
            # run the timer and then kill the query
            logger.debug(
                f"kill_query: datetime.now = {datetime.datetime.now()}, kill_sql = {kill_sql} to be called."
            )
            kill_res = proton_client.execute(kill_sql)
            retry = 100  # kill_query retry 100 times
            while len(kill_res) and retry > 0:
                time.sleep(0.2)
                kill_res = proton_client.execute(kill_sql)
                retry -= 1
                logger.debug(f"kill_query: kill_res = {kill_res} was called")
            if retry <= 0:
                logger.info(
                    f"kill query_id = {query_2_kill}, kill_sql = {kill_sql} cmd executed but failed to kill after 100 times retry."
                )
                return False

            else:
                logger.info(
                    f"kill query_id = {query_2_kill}, kill_sql = {kill_sql} cmd executed and success."
                )
                return True
        except BaseException as error:
            logger.error(f"Kill Query Exception: error = {error}")
            return False

    def get_none_stream_nodes(
        self, config, depends_stream_info_list
    ):  # streams: a string seperated by ","
        logger = mp.get_logger()
        stream_nodes = []
        node_name_list = []
        none_stream_nodes = []
        proton_servers = config.get("proton_servers")

        stream_nodes = self.get_depends_nodes_by_engine(
            config, depends_stream_info_list, "Stream"
        )
        for item in stream_nodes:
            node_name = item.get("node")
            node_name_list.append(node_name)
        for item in proton_servers:
            node_name = item.get("node")
            if node_name not in node_name_list:
                none_stream_nodes.append(node_name)
        logger.debug(
            f"stream_nodes={stream_nodes}, proton_servers = {proton_servers}, depends_stream_info_list = {depends_stream_info_list}, none_stream_nodes = {none_stream_nodes}"
        )
        return none_stream_nodes

    def get_single_stream_only_nodes(self, config, depends_stream_info_list):
        logger = mp.get_logger()
        proton_servers = config.get("proton_servers")
        stream_depends = self.get_depends_nodes_by_engine(
            config, depends_stream_info_list, "Stream"
        )
        all_depends = self.get_depends_nodes_by_engine(config, depends_stream_info_list)
        single_steam_only_depend_nodes = []

        for stream_depend in stream_depends:
            stream_name = stream_depend.get("name")
            stream_node = stream_depend.get("node")
            other_depend_nodes = []
            for (
                item
            ) in (
                all_depends
            ):  # go through the all_depends list to put node of other depends into other_depends_nodes list
                item_name = item.get("name")
                item_node_name = item.get("node")
                item_engine = item.get("engine")
                if item_node_name != stream_node:
                    other_depend_nodes.append(item_node_name)
                elif item_name != stream_name:
                    other_depend_nodes.append(item_node_name)
            for (
                proton_server
            ) in (
                proton_servers
            ):  # go through the proton_servers, if the host == stream_node(the host hosts stream_depend) and not in other_depend_nodes(host does not host any other depends)
                node_name = proton_server.get("host")
                if node_name == stream_node and (node_name not in other_depend_nodes):
                    single_steam_only_depend_nodes.append(node_name)
        logger.debug(
            f"proton_servers = {proton_servers}, depends_stream_info_list = {depends_stream_info_list}, single_steam_only_depend_nodes = {single_steam_only_depend_nodes}"
        )
        return single_steam_only_depend_nodes

    def get_stream_only_nodes(
        self, config, depends_stream_info_list
    ):  # todo: consolidte get_stream_only_node logic and get_view_only_node logic
        logger = mp.get_logger()
        proton_servers = config.get("proton_servers")
        stream_depends = self.get_depends_nodes_by_engine(
            config, depends_stream_info_list, "Stream"
        )
        view_depends = self.get_depends_nodes_by_engine(
            config, depends_stream_info_list, "View"
        )
        stream_only_depend_nodes = []
        stream_depend_nodes = []
        view_depend_nodes = []
        for stream_depend in stream_depends:  # get a list of node hosts stream
            stream_name = stream_depend.get("name")
            stream_node = stream_depend.get("node")
            stream_depend_nodes.append(stream_node)
        for view_depend in view_depends:  # get a list of node hosts view
            view_name = view_depend.get("name")
            view_node = view_depend.get("node")
            view_depend_nodes.append(view_node)
        for item in stream_depend_nodes:
            if item not in view_depend_nodes:
                stream_only_depend_nodes.append(item)
        logger.debug(
            f"proton_servers = {proton_servers}, depends_stream_info_list = {depends_stream_info_list}, stream_only_depend_nodes = {stream_only_depend_nodes}"
        )
        return stream_only_depend_nodes

    def get_view_only_nodes(
        self, config, depends_stream_info_list
    ):  # todo: consolidte get_stream_only_node logic and get_view_only_node logic
        logger = mp.get_logger()
        proton_servers = config.get("proton_servers")
        stream_depends = self.get_depends_nodes_by_engine(
            config, depends_stream_info_list, "Stream"
        )
        view_depends = self.get_depends_nodes_by_engine(
            config, depends_stream_info_list, "View"
        )
        logger.debug(f"view_depends = {view_depends}")
        view_only_depend_nodes = []
        stream_depend_nodes = []
        view_depend_nodes = []
        for stream_depend in stream_depends:  # get a list of node hosts stream
            stream_name = stream_depend.get("name")
            stream_node = stream_depend.get("node")
            stream_depend_nodes.append(stream_node)
        for view_depend in view_depends:  # get a list of node hosts view
            view_name = view_depend.get("name")
            view_node = view_depend.get("node")
            view_depend_nodes.append(view_node)
        for item in view_depend_nodes:
            if item not in stream_depend_nodes:
                view_only_depend_nodes.append(item)
        logger.debug(
            f"proton_servers = {proton_servers}, depends_stream_info_list = {depends_stream_info_list}, view_only_depend_nodes = {view_only_depend_nodes}"
        )
        return view_only_depend_nodes

    def get_host_all_node(self, config, depends_stream_info_list):
        logger = mp.get_logger()
        proton_servers = config.get("proton_servers")
        all_depends = self.get_depends_nodes_by_engine(config, depends_stream_info_list)
        logger.debug(f"all_depends = {all_depends}")
        resource_list = []
        depend_node_list = []
        depend_node_info_list = []
        host_all_nodes = []
        for item in all_depends:
            resource_on_item = item.get("name")
            node_name = item.get("node")
            if resource_on_item not in resource_list:
                resource_list.append(resource_on_item)
            if node_name not in depend_node_list:
                depend_node_list.append(node_name)
                depend_node_info_list.append(
                    {"node": node_name, "resources": [resource_on_item]}
                )
            else:
                for depend_node_info in depend_node_info_list:
                    depend_node = depend_node_info.get("node")
                    if depend_node == node_name:
                        depend_node_info["resources"].append(resource_on_item)
        for (
            depend_node
        ) in (
            depend_node_info_list
        ):  # go through the depend_node_info_list, get the resources on the node and check if all the resoruces are in the list
            host_all_flag = 1
            depend_node_name = depend_node.get("node")
            resources_on_depend_node = depend_node.get("resources")
            for resource in resource_list:
                if resource in resources_on_depend_node:
                    host_all_flag *= 1
                else:
                    host_all_flag *= 0
            if host_all_flag:
                host_all_nodes.append(depend_node_name)
        logger.debug(
            f"proton_servers = {proton_servers}, depends_stream_info_list = {depends_stream_info_list}, host_all_nodes = {host_all_nodes}"
        )
        return host_all_nodes

    def get_host_none_node(self, config, depends_stream_info_list):
        logger = mp.get_logger()
        proton_servers = config.get("proton_servers")
        all_depends = self.get_depends_nodes_by_engine(config, depends_stream_info_list)
        none_depends_nodes = []
        depend_node_list = []
        for item in all_depends:
            node_name = item.get("node")
            depend_node_list.append(node_name)
        for item in proton_servers:
            node_name = item.get("node")
            if node_name not in depend_node_list:
                none_depends_nodes.append(node_name)
        logger.debug(
            f"proton_servers = {proton_servers}, depends_stream_info_list = {depends_stream_info_list}, none_depends_nodes = {none_depends_nodes}"
        )
        return none_depends_nodes

    def get_depends_nodes_by_engine(
        self, config, depends_stream_info_list, engine="All"
    ):
        proton_servers = config.get("proton_servers")
        resource_nodes = []
        view_nodes = []
        for depends_stream_info in depends_stream_info_list:
            resource_name = depends_stream_info.get("name")
            engine_info = depends_stream_info.get("engine")
            if engine_info == "Stream":
                shards = depends_stream_info.get("shards")
                if shards is not None:
                    for shard in shards:
                        replicas = shard.get("replicas")
                        if replicas is not None:
                            for replica in replicas:
                                resource_nodes.append(
                                    {
                                        "name": resource_name,
                                        "engine": engine_info,
                                        "node": replica,
                                    }
                                )
            if "View" in engine_info:
                for item in proton_servers:
                    proton_server = item.get("host")
                    proton_server_native_port = item.get("port")
                    node_name = item.get("node")
                    pyclient = Client(
                        host=proton_server,
                        port=proton_server_native_port,
                        send_receive_timeout=60,
                    )
                    if QueryClientPy.table_exist_py(pyclient, resource_name):
                        resource_nodes.append(
                            {
                                "name": resource_name,
                                "engine": engine_info,
                                "node": node_name,
                            }
                        )
                    pyclient.disconnect()
        if engine == "All":
            return resource_nodes
        else:
            node_list = []
            for item in resource_nodes:
                engine_info = item.get("engine")
                if engine in engine_info:
                    node_list.append(item)
            resource_nodes = node_list
        logger.debug(f"resoruce_nodes = {resource_nodes}")
        return resource_nodes

    def tuple_2_list(self, tuple):
        # transfer tuple to jason string
        _list = []
        for element in tuple:
            if isinstance(element, str):
                _list.append(element)
            else:
                _list.append(str(element))
        return _list

    def run(
        self,
        statement_2_run,
        settings,
        query_results_queue=None,
        config=None,
        pyclient=None,
        logger=None,
        query_states_dict=None,
        telemetry_shared_list=None,
        logging_level="INFO",
    ):
        query_run_start = datetime.datetime.now()
        query_run_py_run_mode = "None"
        proton_setting = {}
        proton_servers = []
        proton_server = None
        proton_server_native_port = None
        proton_cluster_query_node = None
        proton_cluster_query_route_mode = None
        proton_create_stream_shards = None
        proton_create_stream_replicas = None
        proton_server_container_name = None
        rest_setting = None
        table_ddl_url = None
        test_suite_name = statement_2_run.get("test_suite_name")
        test_id = statement_2_run.get("test_id")
        query = statement_2_run.get("query")
        query_id = str(statement_2_run.get("query_id"))
        query_id_type = statement_2_run.get("query_id_type")
        query_type = statement_2_run.get("query_type")
        iter_wait = statement_2_run.get(
            "iter_wait"
        )  # for some slow table query like test_id=61 in materialized_view
        run_mode = statement_2_run.get("run_mode")
        depends_on_stream = statement_2_run.get("depends_on_stream")
        depends_on = statement_2_run.get("depends_on")
        depends_on_done = statement_2_run.get("depends_on_done")
        wait = statement_2_run.get("wait")
        query_start_time_str = str(datetime.datetime.now())
        query_end_time_str = str(datetime.datetime.now())
        query_result_column_types = []
        query_result_list = []
        depends_on_stream_info_list = []
        query_start_time_str = ""
        query_end_time_str = ""
        query_result_column_types = []
        query_result_list = []
        query_results = {}
        if (
            pyclient is None or logger is None
        ):  # if query_run_py is running a standalone process
            logger = mp.get_logger()
        try:
            if config is not None:
                proton_setting = config.get("proton_setting")
                proton_cluster_query_route_mode = config.get(
                    "proton_cluster_query_route_mode"
                )
                proton_cluster_query_node = config.get("proton_cluster_query_node")
                proton_create_stream_shards = config.get("proton_create_stream_shards")
                proton_create_stream_replicas = config.get(
                    "proton_create_stream_replicas"
                )
                proton_server_container_name = config.get(
                    "proton_server_container_name"
                )
                rest_setting = config.get("rest_setting")
                table_ddl_url = rest_setting.get("table_ddl_url")
                if "cluster" not in proton_setting:
                    proton_server = config.get("proton_server")
                    proton_server_native_ports = config.get("proton_server_native_port")
                    proton_server_native_ports = proton_server_native_ports.split(",")
                    proton_server_native_port = proton_server_native_ports[
                        0
                    ]  # todo: get proton_server and port from statement
                elif (
                    "cluster" in proton_setting
                    and proton_cluster_query_node != "default"
                ):
                    proton_servers = config.get("proton_servers")
                    for item in proton_servers:
                        node = item.get("node")
                        if node == proton_cluster_query_node:
                            proton_server = item.get("host")
                            proton_server_native_port = item.get("port")
                    logger.debug(
                        f"proton_cluster_query_node = {proton_cluster_query_node}, proton_server = {proton_server}, proton_server_container_name = {proton_server_container_name}, proton_server_native_port = {proton_server_native_port}"
                    )
                else:  # if 'cluster' in proton_setting 'cluster', go through the list and get the 1st node as default proton
                    proton_servers = config.get("proton_servers")
                    proton_server = proton_servers[0].get("host")
                    proton_server_native_port = proton_servers[0].get("port")
            if pyclient == None:
                query_run_py_run_mode = "process"
                console_handler = logging.StreamHandler(sys.stderr)
                console_handler.formatter = formatter
                logger.addHandler(console_handler)
                if logging_level == "INFO":
                    logger.setLevel(logging.INFO)
                elif logging_level == "DEBUG":
                    logger.setLevel(logging.DEBUG)
                elif logging_level == "ERROR":
                    logger.setLevel(logging.ERROR)
                elif logging_level == "WARNING":
                    logger.setLevel(logging.WARNING)
                elif logging_level == "CRITICAL":
                    logger.setLevel(logging.CRITICAL)
                else:
                    logger.setLevel(logging.INFO)
                logger.debug(
                    f"process started: query_run_py_run_mode = {query_run_py_run_mode}, handler of logger = {logger.handlers}, logger.level = {logger.level}"
                )
                settings = {"max_block_size": 100000}
                pyclient = Client(
                    host=proton_server,
                    port=proton_server_native_port,
                    send_receive_timeout=60,
                )  # create python client
                CLEAN_CLIENT = True
            else:
                query_run_py_run_mode = "local"
                if logger is not None:
                    logger.debug(
                        f"local running: query_run_py_run_mode = {query_run_py_run_mode}, handler of logger = {logger.handlers}, logger.level = {logger.level}"
                    )
            if depends_on_stream != None and isinstance(
                depends_on_stream, str
            ):  # depends_on_stream: a string seperated by "," to indicated streams the query or input depends on
                depends_on_stream_list = depends_on_stream.split(",")
                depends_on_stream_info_list = QueryClientRest.depends_on_stream_exist(
                    table_ddl_url, depends_on_stream_list, query_id
                )
                logger.debug(
                    f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, proton_cluster_query_node = {proton_cluster_query_node},proton_cluster_query_route_mode= {proton_cluster_query_route_mode} "
                )
                if "cluster" in proton_setting:
                    selected_nodes = []  # selected_nodes
                    if (
                        proton_cluster_query_node == "default"
                        and proton_cluster_query_route_mode == NONE_STREAM_NODE_FIRST
                    ):  # if cluster mode and proton_cluster_query_route_mode is set to NONE_STREAM_NODE_FIRST, get none stream node and change pyclient to that node.
                        selected_nodes = self.get_none_stream_nodes(
                            config, depends_on_stream_info_list
                        )
                    elif (
                        proton_cluster_query_node == "default"
                        and proton_cluster_query_route_mode
                        == SINGLE_STREAM_ONLY_NODE_FIRST
                    ):  # try to find the node host only one stream the test case depends on, if can't find the node host only one stream, try to find a node host only stream if the test case depends on streams and views, if can't find initial the query from the default node.
                        selected_nodes = self.get_single_stream_only_nodes(
                            config, depends_on_stream_info_list
                        )
                    elif (
                        proton_cluster_query_node == "default"
                        and proton_cluster_query_route_mode == STREAM_ONLY_NODE_FIRST
                    ):  # try to find the node host only stream the test case depends on, if can't find the node host only one stream, try to find a node host only stream if the test case depends on streams and views, if can't find initial the query from the default node.
                        selected_nodes = self.get_stream_only_nodes(
                            config, depends_on_stream_info_list
                        )
                    elif (
                        proton_cluster_query_node == "default"
                        and proton_cluster_query_route_mode == VIEW_ONLY_NODE_FIRST
                    ):  # try to find a node host only view to initial the query if the test case depends on streams and views, if can't find initial query from the default node
                        selected_nodes = self.get_view_only_nodes(
                            config, depends_on_stream_info_list
                        )
                    elif (
                        proton_cluster_query_node == "default"
                        and proton_cluster_query_route_mode == HOST_ALL_NODE_FIRST
                    ):  # try to find a node host all the streams, views the test case depends on, if can't find initial query from the default node.
                        selected_nodes = self.get_host_all_node(
                            config, depends_on_stream_info_list
                        )
                    elif (
                        proton_cluster_query_node == "default"
                        and proton_cluster_query_route_mode == HOST_NONE_NODE_FIRST
                    ):  # try to find a node does not host any stream or view the test case depends on, if can't find initial query from the default node.
                        selected_nodes = self.get_host_none_node(
                            config, depends_on_stream_info_list
                        )
                    if (
                        len(selected_nodes) != 0
                    ):  # if none_stream_nodes list is not empty, chose the 1st as the proton_server to issue query, if it's empty that means no none_stream_node, so no change to the default proton_server
                        pyclient.disconnect()
                        logger.debug(f"pyclient disconnected")
                        for item in proton_servers:
                            node = item.get("node")
                            if selected_nodes[0] == node:
                                proton_server = item.get("host")
                                proton_server_native_port = item.get("port")
                        settings = {"max_block_size": 100000}
                        logger.debug(
                            f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, selected_nodes = {selected_nodes}, proton_server = {proton_server}, proton_server_native_port = {proton_server_native_port}"
                        )
                        pyclient = Client(
                            host=proton_server,
                            port=proton_server_native_port,
                            send_receive_timeout=60,
                        )  # create python client
            if depends_on != None:  # todo: support depends_on multiple query_id
                running_state_check_res = self.query_state_check(
                    query_states_dict, RUNNING_STATE, test_id, depends_on
                )
                depends_on_exists = False
                depends_on_exists = QueryClientRest.query_exists(
                    depends_on, client=pyclient
                )
                if not depends_on_exists:  # todo: error handling logic and error code
                    error_msg = f"QUERY_DEPENDS_ON_FAILED_TO_START FATAL exception: proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name},test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, depends_on = {depends_on} of query_id = {query_id} does not be found during 30s after {query_id} was started, query_states_dict = {query_states_dict}, raise Fatal Error, the depends_on query may failed to start in 30s or exits/ends unexpectedly."
                    logger.error(error_msg)
                    query_depends_on_exception = TestException(
                        error_msg, ErrorCodes.QUERY_DEPENDS_ON_FAILED_TO_START
                    )
                    raise query_depends_on_exception
                else:
                    logger.info(
                        f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, depends_on = {depends_on} of query_id = {query_id} exists"
                    )
                time.sleep(1)  # for waiting the depends_on query ready.
            if depends_on_done is not None:
                done_state_check_res = self.query_state_check(
                    query_states_dict, DONE_STATE, test_id, depends_on_done
                )
            if (
                proton_create_stream_shards is not None
                and len(proton_create_stream_shards) > 0
                and ("create stream" in query or "CREATE STREAM" in query)
            ):
                q = query.split("settings ")
                if len(q) > 1:
                    shards_str = q[-1] + "," + "shards=" + proton_create_stream_shards
                else:
                    shards_str = "shards=" + proton_create_stream_shards
                query = q[0] + "settings " + shards_str
            if (
                proton_create_stream_replicas is not None
                and len(proton_create_stream_replicas) > 0
                and ("create stream" in query or "CREATE STREAM" in query)
            ):
                q = query.split("settings ")
                if len(q) > 1:
                    replicas_str = (
                        q[-1] + "," + "replicas=" + proton_create_stream_replicas
                    )
                else:
                    replicas_str = "replicas=" + proton_create_stream_replicas
                query = q[0] + "settings " + replicas_str
            if wait != None:
                wait = int(wait)
                time.sleep(wait)
            try:  # do trace_stream logic
                if "trace_stream" in statement_2_run:
                    trace_stream = statement_2_run["trace_stream"]
                    if "trace_id" in trace_stream and "streams" in trace_stream:
                        trace_id = trace_stream["trace_id"]
                        streams_2_trace = trace_stream["streams"].split(",")
                        if "trace_query" in trace_stream:
                            trace_query = trace_stream["trace_query"]
                            for stream in streams_2_trace:
                                query_2_run = trace_query.replace("$", stream)
                                res = pyclient.execute(
                                    query_2_run,
                                    with_column_types=True,
                                    settings=settings,
                                )
                                if logger is not None:
                                    logger.info(
                                        f"trace_stream_check, proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  trace_id = {trace_id}, stream_2_trace = {stream}, trace_result = {res}"
                                    )
            except (
                BaseException
            ) as error:  # catch trace_stream error and log and no impact to the test execution
                logger.error(f"trace_stream exception, error = {error}")
                traceback.print_exc()
            if query_type == "table":
                query_result_iter = []
                if "cluster" in proton_setting and (
                    "kill query" in query or "KILL QUERY" in query
                ):
                    if (
                        "kill query" in query or "KILL QUERY" in query
                    ):  # todo: better logic for the manual kill query in cluster
                        res = None
                        for item in proton_servers:
                            proton_server = item.get("host")
                            proton_server_native_port = item.get("port")
                            if pyclient is not None:
                                pyclient.disconnect()
                            logger.debug(f"pyclient disonnected.")
                            pyclient = Client(
                                host=proton_server,
                                port=proton_server_native_port,
                                send_receive_timeout=60,
                            )  # create python client
                            logger.debug(
                                f"proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name},test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query = {query} run on proton_server={proton_server}, proton_server_native_port = {proton_server_native_port}"
                            )
                            res = pyclient.execute(
                                query,
                                with_column_types=True,
                                query_id=query_id,
                                settings=settings,
                            )
                            query_result_iter.append(res[-1])
                            if len(res[0]) > 0:
                                for item in res[0]:
                                    query_result_iter.append(item)
                        logger.debug(
                            f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, query_type = {query_type}, res={res}, query_result_iter = {query_result_iter}"
                        )
                        pyclient.disconnect()
                else:
                    res = pyclient.execute(
                        query,
                        with_column_types=True,
                        query_id=query_id,
                        settings=settings,
                    )
                    query_result_iter.append(res[-1])
                    if len(res[0]) > 0:
                        for item in res[0]:
                            query_result_iter.append(item)
                    logger.debug(
                        f"proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name},test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query_type = {query_type}, res={res}, query_result_iter = {query_result_iter}"
                    )
            else:
                query_result_iter = pyclient.execute_iter(
                    query, with_column_types=True, query_id=query_id, settings=settings
                )
            logger.debug(
                f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, proton_server = {proton_server}, proton_server_native_port = {proton_server_native_port}, query_run_py: query_id = {query_id}, executed @ {str(datetime.datetime.now())}, query = {query}......"
            )
            if (query_type != None and query_type == "table") and (iter_wait != None):
                iter_wait = int(iter_wait)
                time.sleep(
                    iter_wait
                )  # sleep for materialized_view test_id = 61, the execute_iter is async way, if too quick to start to iter, wait for 1s until query is setup, need to observe

                logger.debug(
                    f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, query_type = {query_type}, sleep for iter_wait = {iter_wait}s"
                )
            if (
                query_type != None
                and query_type == "table"
                and "CREATE" in str.upper(query)
            ):  # call depends_on_stream_exist to check if stream/view is created automatically if the statement is a create statement
                create_exist_res = QueryClientPy.scan_statement_and_act(
                    statement_2_run, "exist", pyclient, config
                )
            i = 0
            for element in query_result_iter:
                logger.debug(
                    f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, element got @ t{str(datetime.datetime.now())} in query_result_iter in query_id: {query_id} = {element}"
                )
                if isinstance(element, list) or isinstance(element, tuple):
                    element = list(element)
                if i == 0:
                    query_result_column_types = element
                    if query_states_dict is not None:
                        query_states_dict[str(test_id)] = {str(query_id): RUNNING_STATE}
                else:
                    element_list = self.tuple_2_list(element)
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
                "query_id_type": query_id_type,
            }
            logger.info(
                f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, query_run_py: query_results of query={query} = {query_results}"
            )
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
                        {
                            "statement_2_run": statement_2_run,
                            "time_spent": time_spent_ms,
                        }
                    )
                else:
                    pass  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.
                pyclient.disconnect()
        except (BaseException, errors.ServerException) as error:
            error_string = f"query_run_py, exception, error = {error}"  # todo: handle the error code but not string match
            if isinstance(error, errors.NetworkError) or isinstance(
                error, errors.SocketTimeoutError
            ):
                logger.error(
                    f"crash, connection failure,proton_server_container_name = {proton_server_container_name}, proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query_run_py, query_run_py_run_mode = {query_run_py_run_mode}, exception, query_id={query_id}, query={query}, error = {error}"
                )
                traceback.print_exc()
                query_results = {
                    "query_id": query_id,
                    "query": query,
                    "query_type": query_type,
                    "query_state": "crash",
                    "query_start": query_start_time_str,
                    "query_end": query_end_time_str,
                    "query_result": f"error_code:10000, error: {error}"
                    if error.code == None
                    else f"error_code:{error.code}, error: {error}",
                    "query_id_type": query_id_type,
                    "error": error_string,
                }
                # if proton crashes, send 10000 as error_code
                message_2_send = json.dumps(query_results)
                if query_results_queue != None:
                    query_results_queue.put(message_2_send)
                if run_mode == "process" or query_type == "stream":
                    logger.debug(
                        f"QUERY_RUN_ERROR CRASH exception: proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id},  query_id = {query_id}, query={query}, query_results = {query_results}"
                    )
                    if telemetry_shared_list != None:
                        telemetry_shared_list.append(
                            {
                                "statement_2_run": statement_2_run,
                                "time_spent": 0,
                            }
                        )
                    else:
                        pass  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.
                    if pyclient is not None:
                        pyclient.disconnect()
            # if no crash but other fatal error.
            elif isinstance(error, TestException):
                logger.error(
                    f"QUERY_RUN_ERROR FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, query_run_py, query_run_py_run_mode = {query_run_py_run_mode}, query_id={query_id}, query={query}, error = {error}"
                )
                traceback.print_exc()
                query_results = {
                    "query_id": query_id,
                    "query": query,
                    "query_type": query_type,
                    "query_state": "fatal",
                    "query_start": query_start_time_str,
                    "query_end": query_end_time_str,
                    "query_result": f"error_code:10000, error: {error}"
                    if error.code == None
                    else f"error_code:{error.code}, error: {error}",
                    "query_id_type": query_id_type,
                    "error": error_string,
                }
                message_2_send = json.dumps(query_results)
                if query_results_queue != None:
                    query_results_queue.put(message_2_send)
                if run_mode == "process" or query_type == "stream":
                    if telemetry_shared_list != None:
                        telemetry_shared_list.append(
                            {
                                "statement_2_run": statement_2_run,
                                "time_spent": 0,
                            }
                        )
                    else:
                        pass  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.
                    if pyclient is not None:
                        pyclient.disconnect()
            else:
                print(
                    f"proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name},test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query_run_py, query_run_py_run_mode = {query_run_py_run_mode}, exception, query_id={query_id}, query={query}, exception = {error}"
                )
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
                            "query_id_type": query_id_type,
                        }
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
                            "query_id_type": query_id_type,
                        }
                        message_2_send = json.dumps(query_results)
                        if query_results_queue != None:
                            query_results_queue.put(message_2_send)
                    if run_mode == "process" or query_type == "stream":
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
                            pass  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.
                        if pyclient is not None:
                            pyclient.disconnect()
                else:
                    query_results = {
                        "query_id": query_id,
                        "query": query,
                        "query_type": query_type,
                        "query_state": "exception",
                        "query_start": query_start_time_str,
                        "query_end": query_end_time_str,
                        "query_result": f"error_code:10000, error: {error}"
                        if error.code == None
                        else f"error_code:{error.code}, error: {error}",
                        "query_id_type": query_id_type,
                    }
                    traceback.print_exc()
                    # if it's not db excelption, send 10000 as error_code
                    message_2_send = json.dumps(query_results)
                    if query_results_queue != None:
                        query_results_queue.put(message_2_send)

                if run_mode == "process" or query_type == "stream":
                    if telemetry_shared_list != None:
                        telemetry_shared_list.append(
                            {"statement_2_run": statement_2_run, "time_spent": 0}
                        )
                    else:
                        pass  # todo: put the telemetry data into return, telemetry_shared_list=None means query_run_py is called by query_execute directly but not in child process.
                    if pyclient is not None:
                        pyclient.disconnect()
        finally:
            for act in ACTS_IN_FINAL:
                scan_statement_act_res = QueryClientPy.scan_statement_and_act(
                    statement_2_run, act, pyclient
                )
                logger.debug(
                    f"watch: scan_statement_act_res = {scan_statement_act_res}"
                )
            if query_run_py_run_mode == "local":
                logger.debug(
                    f"local run shut down: query_run_py_run_mode = {query_run_py_run_mode}."
                )
            elif query_run_py_run_mode == "process":
                logger.debug(
                    f"process shut down: query_run_py_run_mode = {query_run_py_run_mode}."
                )
            else:
                logger.debug(f"close: query_run_py_run_mode = {query_run_py_run_mode}.")
            return query_results


class QueryClientRest(QueryClient):
    def __init__(self, type=QueryClientType.REST, **query_client_context):
        super().__init__(type, **query_client_context)

    def is_json(self, string):
        try:
            json_obj = json.loads(string)
            return True
        except ValueError as error:
            return False

    def request_rest(
        self,
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
        except BaseException as error:
            logger.error(f"exception, error= {error}")
            traceback.print_exc()
            return None

    @classmethod
    def input_batch_rest(
        cls,
        config,
        test_suite_name,
        test_id,
        input_batch,
        table_schema,
        query_states_dict,
    ):
        # todo: complete the input by rest
        logger = mp.get_logger()
        input_batch_record = {}
        input_rest_columns = []
        input_rest_body_data = []
        input_rest_body = {"columns": input_rest_columns, "data": input_rest_body_data}
        proton_setting = config.get("proton_setting")
        running_state_check_res = None
        try:
            logger.debug(
                f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, input_batch_rest: input_batch = {input_batch}, table_schema = {table_schema}, input_batch_rest starts."
            )
            rest_setting = config.get("rest_setting")
            input_url = rest_setting.get("ingest_url")
            query_url = rest_setting.get("query_url")
            table_ddl_url = rest_setting.get("table_ddl_url")
            wait = input_batch.get("wait")
            # table_name = table_schema.get("name")
            table_name = input_batch.get("table_name")
            if table_name == None:
                error_msg = f"Test_CASE_INPUT_TABLE_NAME_NOT_FOUND: test_suite_name = {test_suite_name}, test_id = {test_id}, table_name of input_batch is None"
                test_case_input_table_name_exception = TestException(
                    error_msg, ErrorCodes.TEST_CASE_INPUT_TABLE_NAME_NOT_FOUND
                )
                raise test_case_input_table_name_exception
            columns = input_batch.get("columns")
            if columns == None and table_schema == None:
                return []
            depends_on_stream_info_list = []
            depends_on_stream_list = []
            depends_on_stream = input_batch.get("depends_on_stream")
            if depends_on_stream != None:
                depends_on_stream_list = depends_on_stream.split(",")
            depends_on_stream_list.append(
                table_name
            )  # add table name of the input into the depends_on_stream list.
            depends_on_stream_info_list = QueryClientRest.depends_on_stream_exist(
                table_ddl_url, depends_on_stream_list
            )
            depends_on = input_batch.get("depends_on")
            depends_on_exists = False
            if depends_on != None:
                logger.debug(f"depends_on = {depends_on}, checking...")
                running_state_check_res = QueryClient.query_state_check(
                    query_states_dict, RUNNING_STATE, test_id, depends_on
                )
                if "cluster" not in proton_setting:
                    depends_on_exists = QueryClientRest.query_exists(
                        depends_on, query_url
                    )
                else:  # if proton_setting == 'cluster', go through all the proton_servers to check if query exists
                    proton_servers = config.get("proton_servers")
                    depends_on_exists_flag = 0
                    retry = 100
                    while (
                        not depends_on_exists and retry > 0
                    ):  # todo: consolidate this cluster query exist into a cluster_query_exists function or query_exists()
                        for item in proton_servers:
                            proton_server = item.get("host")
                            proton_server_native_port = item.get("port")
                            # logger.debug(
                            #     f"proton_server={proton_server}, proton_server_native_port = {proton_server_native_port}"
                            # )
                            pyclient = Client(
                                host=proton_server,
                                port=proton_server_native_port,
                                send_receive_timeout=60,
                            )
                            if QueryClientPy.query_id_exists_py(pyclient, depends_on):
                                depends_on_exists_flag += 1
                        if depends_on_exists_flag > 0:
                            depends_on_exists = True
                        else:
                            depends_on_exists = False
                        time.sleep(0.2)
                        retry -= 1
                time.sleep(
                    1
                )  # wait 1 seconds for dpends_on query ready, if no wait sometimes data will be missed in the query
                if not depends_on_exists:
                    error_msg = f"QUERY_INPUT_DEPENDS_ON_NOT_EXIST FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, depends_on = {depends_on}, query_states_dict = {query_states_dict} for input not found, raise exception."
                    logger.error(error_msg)
                    query_input_depends_on_exception = TestException(
                        error_msg, ErrorCodes.QUERY_INPUT_DEPENDS_ON_NOT_EXIST
                    )
                    raise query_input_depends_on_exception
            else:
                running_state_check_res = "no_depends_on"
            depends_on_done = input_batch.get("depends_on_done")
            if depends_on_done is not None:
                done_state_check_res = QueryClient.query_state_check(
                    query_states_dict, DONE_STATE, test_id, depends_on
                )
            else:
                done_state_check_res = "no_depends_on_done"
            if wait != None:
                # logger.debug(f"wait for {wait}s to start inputs.")
                wait = int(wait)
                # logger.info(f"sleep {wait} before input")
                time.sleep(wait)
            if columns != None:
                for each in columns:
                    input_rest_columns.append(each)
            elif table_schema != None:
                for element in table_schema.get("columns"):
                    input_rest_columns.append(element.get("name"))
            input_batch_data = input_batch.get("data")
            for row in input_batch_data:
                # logger.debug(f"input_batch_rest: row_data = {row}")
                input_rest_body_data.append(
                    row
                )  # get data from inputs batch dict as rest ingest body.
            input_rest_body = json.dumps(input_rest_body)
            input_url = f"{input_url}/{table_name}"
            res = requests.post(input_url, data=input_rest_body)
            logger.debug(
                f"input_batch_rest: response of requests.post of input_url = {input_url}, data = {input_rest_body}, running_state_check_res = {running_state_check_res},done_state_check_res = {done_state_check_res}, request res = {res}"
            )
            assert res.status_code == 200, f"res.status_code = {res.status_code}"
            input_batch_record["input_batch"] = input_rest_body_data
            input_batch_record["timestamp"] = str(datetime.datetime.now())
            logger.debug(f"input_batch done succesfully...")
            return input_batch_record
        except BaseException as error:
            if not isinstance(error, TestException):
                error_msg = f"INPUT_BATCH_REST_ERROR FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id},running_state_check_res = {running_state_check_res},done_state_check_res = {done_state_check_res},  error = {error}"
                logger.error(error_msg)
                traceback.print_exc()
                raise TestException(error_msg, ErrorCodes.INPUT_BATCH_REST_ERROR)
            else:
                raise error

    @classmethod
    def query_exists(
        cls, query_id, query_url=None, client=None
    ):  # todo: adopt query_id_exists_py and query_id_exists_rest
        logger = mp.get_logger()
        query_exists_timeout = 20  # todo: test_suite_env_setup_timeout in config file
        query_esists_timeout_hit = (
            threading.Event()
        )  # set the test_suite_timeout_hit flag as False and start a timer to set this flag
        timer = threading.Timer(
            query_exists_timeout,
            timeout_flag,
            [
                query_esists_timeout_hit,
                "QUERY_EXISTS_TIMEOUT_ERROR FATAL and set the timeout treading event",
                f"query_id = {query_id}",
            ],
        )
        timer.start()
        logger.debug(
            f"query_id = {query_id}, query_exists_timeout = {query_exists_timeout}, timer started."
        )
        query_id_list = []
        query_id_exists = False
        retry = 100  # change from 300 to 100
        logger.debug(f"checking query_id = {query_id} if exists...")
        while (
            not query_id_exists and retry > 0 and not query_esists_timeout_hit.is_set()
        ):
            if client == None:
                try:
                    query_id_exists = QueryClientRest.query_id_exists_rest(
                        query_url, query_id
                    )
                except BaseException as error:
                    error_msg = f"QUERY_EXIST_CHECK_REST_FAILED exception, query_url = {query_url}, query_id = {query_id}, error = {error}"
                    logger.error(error_msg)
                    query_exist_check_rest_exception = TestException(
                        error_msg, ErrorCodes.QUERY_EXIST_CHECK_REST_FAILED
                    )
                    traceback.print_exc()
                    raise query_exist_check_rest_exception
            else:
                try:
                    query_id_exists = QueryClientPy.query_id_exists_py(client, query_id)
                except BaseException as error:
                    error_msg = f"QUERY_EXIST_CHECK_NATIVE_FAILED exception, query_url = {query_url}, query_id = {query_id}, error = {error}"
                    logger.error(error_msg)
                    query_exist_check_native_exception = TestException(
                        error_msg, ErrorCodes.QUERY_EXIST_CHECK_NATIVE_FAILED
                    )
                    traceback.print_exc()
                    raise query_exist_check_native_exception
            if query_id_exists:
                logger.debug(f"query_id = {query_id}, found")
                timer.cancel()
                return True
            else:
                time.sleep(0.1)
                retry -= 1
        if query_id_exists:
            logger.debug(f"query_id = {query_id}, found")
            timer.cancel()
            return True
        else:
            logger.debug(f"query_id = {query_id}, not found")
            timer.cancel()
            return False

    @classmethod
    def depends_on_stream_exist(
        cls, table_ddl_url, depends_on_stream_list, query_id=None
    ):
        depends_on_stream_exist_timeout = (
            50  # todo: test_suite_env_setup_timeout in config file
        )
        depends_on_stream_exist_timeout_hit = (
            threading.Event()
        )  # set the test_suite_timeout_hit flag as False and start a timer to set this flag
        timer = threading.Timer(
            depends_on_stream_exist_timeout,
            timeout_flag,
            [
                depends_on_stream_exist_timeout_hit,
                "DEPENDS_ON_STREAM_EXIST_TIMEOUT_ERROR FATAL and set the timeout treading event",
                f"table_ddl_url = {table_ddl_url}, depends_on_stream_list = {depends_on_stream_list}",
            ],
        )
        timer.start()
        logger.debug(
            f"table_ddl_url = {table_ddl_url}, depends_on_stream_list = {depends_on_stream_list}, depends_on_stream_exist_timeout = {depends_on_stream_exist_timeout}, timer started."
        )
        table_info_list = []
        depends_on_stream_list = list(
            set(depends_on_stream_list)
        )  # dedup depends_on_stream_list in case of duplicate depends_on_stream
        for depends_on_stream in depends_on_stream_list:
            retry = 100
            table_info = QueryClientRest.table_exist(table_ddl_url, depends_on_stream)
            logger.debug(f"start to check depends_on_stream = {depends_on_stream}")
            while (
                table_info is None
                and retry > 0
                and not depends_on_stream_exist_timeout_hit.is_set()
            ):
                time.sleep(0.2)
                table_info = QueryClientRest.table_exist(
                    table_ddl_url, depends_on_stream
                )
                retry -= 1
            logger.debug(f"retry remains after retry -=1: {retry}")
            if retry <= 0 or depends_on_stream_exist_timeout_hit.is_set():
                error_msg = f"STREAM_DEPENDS_ON_NOT_EXIST FATAL exception: check stream name of create or drop or input or depends_on_stream of statements 100 times and stream={depends_on_stream} does not exist"
                stream_depends_on_exception = TestException(
                    error_msg, ErrorCodes.STREAM_DEPENDS_ON_NOT_EXIST
                )
                logger.error(error_msg)
                timer.cancel()
                raise stream_depends_on_exception
            else:
                logger.debug(
                    f"check stream name of create or drop or input or depends_on_stream, stream={depends_on_stream} found."
                )
                table_info_list.append(table_info)
        print(f"check depends_on_stream_list = {depends_on_stream_list}, all found")
        timer.cancel()
        return table_info_list

    @classmethod
    def table_exist(cls, table_ddl_url, table_name):
        logger = mp.get_logger()
        logger.debug(
            f"table_exist: table_ddl_url = {table_ddl_url}, table_name = '{table_name}'"
        )
        res = requests.get(table_ddl_url)
        logger.debug(f"table_exist: res.status_code = {res.status_code}")
        if res.status_code == 200:
            res_json = res.json()
            table_list = res_json.get("data")
            # logger.debug(f"table_list = {table_list}")
            if len(table_list) > 0:
                for element in table_list:
                    element_name = element.get("name")
                    # logger.debug(f"table_exist: element_name = {element_name}, table_name = {table_name}")
                    if element_name == table_name:
                        logger.debug(
                            f"table_exist: table_name = {table_name} exists, return table info."
                        )
                        logger.debug(f"element = {element}")
                        return element
                logger.debug(f"table_name = {table_name} does not exist, return None")
                return None
            else:
                logger.debug(f"no table on proton, return None")
                return None
        else:
            logger.info(
                f"Error: table list rest access failed, requests.get({table_ddl_url}), status code={res.status_code},res_json = {res.json()}"
            )
            return None

    @classmethod
    def create_table_rest(cls, config, table_schema, retry=3):
        logger = mp.get_logger()
        rest_setting = config.get("rest_setting")
        proton_setting = config.get("proton_setting")
        test_suite_name = config.get("test_suite_name")
        table_ddl_url = rest_setting.get("table_ddl_url")
        proton_create_stream_shards = config.get("proton_create_stream_shards")
        proton_create_stream_replicas = config.get("proton_create_stream_replicas")
        exception_retry = retry  # set the retry times of exception catching
        table_name = table_schema.get("name")
        type = table_schema.get("type")
        create_start_time = datetime.datetime.now()
        res = None
        while retry > 0 and exception_retry > 0:
            query_parameters = table_schema.get("query_parameters")
            try:
                logger.debug(f"create_table_rest starts...")
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
                if proton_create_stream_shards is not None:
                    table_schema_for_rest["shards"] = int(
                        proton_create_stream_shards
                    )  # if proton_create_stream_shards is not None, put it into table_schema for rest api to create stream with shards setting
                if proton_create_stream_replicas is not None:
                    table_schema_for_rest["replication_factor"] = int(
                        proton_create_stream_replicas
                    )
                if event_time_column is not None:
                    table_schema_for_rest["event_time_column"] = event_time_column
                if ttl_expression is not None:
                    table_schema_for_rest["ttl_expression"] = ttl_expression
                post_data = json.dumps(table_schema_for_rest)
                logger.debug(
                    f"table_create_url = {table_create_url}, data = {post_data} to be posted."
                )
                res = requests.post(
                    table_create_url, data=post_data
                )  # create the table w/ table schema
                if res.status_code == 200:
                    logger.info(
                        f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, table {table_name} create_rest is called successfully."
                    )
                    break
                else:
                    logger.info(
                        f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, table {table_name} create_rest fails, res.status_code = {res.status_code}"
                    )
                    retry -= 1
                    if retry <= 0:
                        return res
                    time.sleep(1)
                    continue
            except BaseException as error:
                error_msg = f"STREAM_CREATE_FAILED FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, create_table_rest, rest api exception: {error}"
                logging.error(error_msg)
                traceback.print_exc()
                exception_retry -= 1
                if exception_retry <= 0:
                    stream_create_failed_exception = TestException(
                        error_msg, ErrorCodes.STREAM_CREATE_FAILED
                    )
                    raise stream_create_failed_exception
                time.sleep(1)
        exception_retry = retry  # reset exception_retry for table_exit check
        create_table_time_out = 200  # set how many times wait and list table to check if table creation completed.
        while create_table_time_out > 0 and exception_retry > 0:
            try:
                if QueryClientRest.table_exist(table_ddl_url, table_name):
                    logger.info(f"table {table_name} is created successfully.")
                    create_complete_time = datetime.datetime.now()
                    time_spent = create_complete_time - create_start_time
                    time_spent_ms = time_spent.total_seconds() * 1000
                    # time_spent_ms = (1000-create_table_time_out) * 10
                    logger.info(
                        f"{time_spent_ms} ms spent on table {table_name} creating"
                    )
                    global TABLE_CREATE_RECORDS
                    TABLE_CREATE_RECORDS.append(
                        {"table_name": table_name, "time_spent": time_spent_ms}
                    )
                    break
                else:
                    time.sleep(0.1)
                create_table_time_out -= 1

                if create_table_time_out <= 0:
                    error_msg = f"STREAM_CREATE_FAILED FATAL exception: proton_setting={proton_setting}, test_suite_name = {test_suite_name}, table_ddl_url = {table_ddl_url}, table_name = {table_name}"
                    logger.error(error_msg)
                    stream_create_failed_exception = TestException(
                        error_msg, ErrorCodes.STREAM_CREATE_FAILED
                    )
                    raise stream_create_failed_exception
            # time.sleep(1) # wait the table creation completed
            except BaseException as error:
                error_msg = (
                    f"STREAM_CREATE_FAILED FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, create_table_rest, rest api exception: {error}"
                    if not isinstance(error, TestException)
                    else f"{error}"
                )
                logging.error(error_msg)
                traceback.print_exc()
                exception_retry -= 1
                if exception_retry <= 0:
                    stream_create_failed_exception = (
                        TestException(error_msg, ErrorCodes.STREAM_CREATE_FAILED)
                        if not isinstance(error, TestException)
                        else error
                    )
                    raise stream_create_failed_exception
                time.sleep(1)
        return res

    @classmethod
    def drop_table_if_exist_rest(
        cls, table_ddl_url, table_name
    ):  # todo: combine drop_table_if_exist_rest and drop_table_if_exist_py to drop_table_if_exist, therefore try python drop when rest drop fail.
        logger = mp.get_logger()
        drop_start_time = datetime.datetime.now()
        try:
            if QueryClientRest.table_exist(table_ddl_url, table_name):
                res = requests.delete(f"{table_ddl_url}/{table_name}")
                logger.debug(f"drop stream if exists {table_name} res = {res}")
                if res.status_code != 200:
                    error_msg = f"DROP_STREAM_REST_ERROR FATAL exception: url = {table_ddl_url}, table_name = {table_name}, res = {res}."
                    drop_stream_rest_exception = TestException(
                        error_msg, ErrorCodes.DROP_STREAM_REST_ERROR
                    )
                    raise drop_stream_rest_exception
                else:
                    # time.sleep(1)  # sleep to wait the table drop completed.
                    logger.info(
                        "drop stream {} is successfully called".format(table_name)
                    )
                check_count = 200
                check_interval = 0.1
                time.sleep(check_interval)
                while (
                    QueryClientRest.table_exist(table_ddl_url, table_name)
                    and check_count > 0
                ):
                    time.sleep(check_interval)
                    check_count -= 1
                if check_count <= 0:
                    error_msg = f"DROP_STREAM_REST_ERROR FATAL exception: url = {table_ddl_url}, table_name = {table_name} still exists 20s after drop stream rest is made."
                    logger.error(error_msg)
                    drop_stream_rest_exception = TestException(
                        error_msg, ErrorCodes.DROP_STREAM_REST_ERROR
                    )
                    raise drop_stream_rest_exception
            logger.debug(
                f"QueryClientRest.table_exist({table_ddl_url}, {table_name}) == None, drop table bypass"
            )
            drop_complete_time = datetime.datetime.now()
            time_spent = drop_complete_time - drop_start_time
            time_spent_ms = time_spent.total_seconds() * 1000
            global TABLE_DROP_RECORDS
            TABLE_DROP_RECORDS.append(
                {"table_name": table_name, "time_spent": time_spent_ms}
            )
            logger.info(f"table {table_name} is dropped, {time_spent_ms} spent.")
        except BaseException as error:
            error_msg = (
                f"DROP_STREAM_REST_ERROR FATAL exception: url = {table_ddl_url}, table_name = {table_name}, error = {error}."
                if not isinstance(error, TestException)
                else f"{error}"
            )
            logger.error(error_msg)
            traceback.print_exc()
            drop_stream_rest_exception = (
                TestException(error_msg, ErrorCodes.DROP_STREAM_REST_ERROR)
                if not isinstance(error, TestException)
                else error
            )
            raise drop_stream_rest_exception

    @classmethod
    def query_id_exists_rest(cls, query_url, query_id, query_body=None):
        logger = mp.get_logger()
        query_id = str(query_id)
        query_id_exist_bool = False
        try:
            query_body = json.dumps(
                {
                    "query": f"select query_id from system.processes where query_id = '{query_id}'"
                }
            )
            res = requests.post(query_url, data=query_body)
            logger.debug(
                f"query_id exists check: query_url = {query_url}, res.status_code = {res.status_code}"
            )
            if res.status_code != 200:
                return False
            res_json = res.json()
            query_id_match_list = []
            query_id_match_list = res_json.get("data")
            if query_id_match_list is not None and isinstance(
                query_id_match_list, list
            ):
                if len(query_id_match_list) > 0:
                    for element in query_id_match_list:
                        logger.debug(f"element = {element}, query_id = {query_id}")
                        if query_id in element:
                            query_id_exist_bool = True
            logger.debug(
                f"query_id search for {query_id} result: query_id_match_list = {query_id_match_list},query_id_exist_bool = {query_id_exist_bool}"
            )
            return query_id_exist_bool
        except BaseException as error:
            logger.error(f"query_id_exists_rest, exception, Error = {error}")
            traceback.print_exc()
            return False

    def run(self, rest_setting, statement_2_run):
        logger = mp.get_logger()
        logger.debug(
            f"local running: handler of logger = {logger.handlers}, logger.level = {logger.level}"
        )
        query_results = {}
        query_id = str(statement_2_run.get("query_id"))
        query_id_type = statement_2_run.get("query_id_type")
        query_type = statement_2_run.get("query_type")
        query = statement_2_run.get("query")
        depends_on_stream = statement_2_run.get("depends_on_stream")
        test_suite_name = statement_2_run.get("test_suite_name")
        test_id = statement_2_run.get("test_id")
        query_start_time_str = str(datetime.datetime.now())
        query_end_time_str = str(datetime.datetime.now())
        query_result_str = ""
        query_result_column_types = []
        query_result_json = {}
        query_result_list = []
        query_result = ""
        query_results = {}
        rest_request = ""
        proton_setting = statement_2_run.get("proton_setting")
        host_url = rest_setting.get("host_url")
        http_snapshot_url = rest_setting.get("http_snapshot_url")
        rest_type = statement_2_run.get("rest_type")
        query_url = statement_2_run.get("query_url")
        table_ddl_url = rest_setting.get("table_ddl_url")
        wait = statement_2_run.get("wait")
        http_method = statement_2_run.get("http_method")
        data = statement_2_run.get("data")
        args = statement_2_run.get("args")
        try:
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
                depends_on_stream_list = depends_on_stream.split(",")
                depends_on_stream_info_list = QueryClientRest.depends_on_stream_exist(
                    table_ddl_url, depends_on_stream_list, query_id
                )
            if depends_on != None:
                depends_on_exists = False
                depends_on_exists = QueryClientRest.query_exists(depends_on, query_url)
                if not depends_on_exists:
                    error_msg = f"QUERY_DEPENDS_ON_NOT_FOUND FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, depends_on = {depends_on} does not exist"
                    logger.debug(error_msg)
                    query_depends_on_not_found_exception = TestException(
                        error_msg, ErrorCodes.QUERY_DEPENDS_ON_FAILED_TO_START
                    )
                    raise query_depends_on_not_found_exception
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
            res = self.request_rest(http_method, url, rest_type, params, data, headers)
            query_end_time_str = str(datetime.datetime.now())
            if res is not None:
                logger.debug(
                    f"rest_request({rest_request}) called, res.status_code = {res.status_code}."
                )
            else:
                logger.debug(f"rest_request({rest_request}) called, res = None.")
            if res is not None:
                if self.is_json(res.text):
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
                "query_id_type": query_id_type,
            }
        except BaseException as error:
            error_string = f"query_run_rest, exception, error = {error}"  # todo: handle the error code but not string match
            traceback.print_exc()
            if isinstance(error, errors.NetworkError) or isinstance(
                error, errors.SocketTimeoutError
            ):
                logger.error(
                    f"crash, connection failure, proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query_run_rest, exception, query_id={query_id}, query={query}, error = {error}"
                )
                query_results = {
                    "query_id": query_id,
                    "query": query,
                    "query_type": query_type,
                    "query_state": "crash",
                    "query_start": query_start_time_str,
                    "query_end": query_end_time_str,
                    "query_result": f"error_code:10000, error: {error}"
                    if error.code == None
                    else f"error_code:{error.code}, error: {error}",
                    "query_id_type": query_id_type,
                    "error": error_string,
                }
            elif isinstance(error, TestException):
                logger.error(
                    f"QUERY_RUN_ERROR FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query_run_rest, query_id={query_id}, query={query}, error = {error}"
                )
                query_results = {
                    "query_id": query_id,
                    "query": query,
                    "query_type": query_type,
                    "query_state": "fatal",
                    "query_start": query_start_time_str,
                    "query_end": query_end_time_str,
                    "query_result": f"error_code:10000, error: {error}"
                    if error.code == None
                    else f"error_code:{error.code}, error: {error}",
                    "query_id_type": query_id_type,
                    "error": error_string,
                }
            else:
                logger.error(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, query_run_rest, exception, query_id={query_id}, query={query}, error = {error}"
                )
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
                    "query_id_type": query_id_type,
                }
        finally:
            logger.debug(f"query_results = {query_results}")
            return query_results


class QueryClientExec(QueryClient):
    def __init__(self, type=QueryClientType.EXEC, **query_client_context):
        super().__init__(type, **query_client_context)

    def run(self, statement_2_run, config):
        logger = mp.get_logger()
        logger.debug(
            f"local running: handler of logger = {logger.handlers}, logger.level = {logger.level}"
        )
        proton_setting = config.get("proton_setting")
        rest_setting = config.get("rest_setting")
        query_url = rest_setting.get("query_url")
        query_results = {}
        query_id = str(statement_2_run.get("query_id"))
        query_id_type = statement_2_run.get("query_id_type")
        query_type = statement_2_run.get("query_type")
        echo = statement_2_run.get("echo")
        query = statement_2_run.get("query")
        test_suite_name = statement_2_run.get("test_suite_name")
        test_id = statement_2_run.get("test_id")
        query_client = statement_2_run.get("client")
        depends_on = statement_2_run.get("depends_on")
        depends_on_stream = statement_2_run.get("depends_on_stream")
        query_start_time_str = str(datetime.datetime.now())
        query_end_time_str = str(datetime.datetime.now())
        user = statement_2_run.get("user")
        password = statement_2_run.get("password")
        query_result_str = ""
        query_result_column_types = []
        query_results = {}
        rest_request = ""
        proton_server_container_name = config.get("proton_server_container_name")
        proton_server_container_name_list = proton_server_container_name.split(",")
        rest_setting = config.get("rest_setting")
        table_ddl_url = rest_setting.get("table_ddl_url")
        commands = []
        for proton_server_container_name in proton_server_container_name_list:
            if query_type is not None and query_type == "docker":
                if echo is not None:
                    echo_str = json.dumps(echo)
                    command = f"echo '{echo_str}' | {query}"
                else:
                    command = f"{query}"
            else:
                command = f'docker exec {proton_server_container_name} proton-client --host 127.0.0.1 -u {user} --password {password} --query="{query}"'
            commands.append(command)
        logger.debug(f"commands = {commands}")
        try:
            if depends_on_stream != None:
                proton_server = config.get("proton_server")
                depends_on_stream_list = depends_on_stream.split(",")
                depends_on_stream_info_list = QueryClientRest.depends_on_stream_exist(
                    table_ddl_url, depends_on_stream_list, query_id
                )
            if depends_on != None:  # todo: support depends_on multiple query_id
                depends_on_exists = False
                depends_on_exists = QueryClientRest.query_exists(depends_on, query_url)
                if not depends_on_exists:  # todo: error handling logic and error code
                    error_msg = f"QUERY_DEPENDS_ON_FAILED_TO_START FATAL exception: proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name},test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, depends_on = {depends_on} of query_id = {query_id} does not be found during 30s after {query_id} was started, query_states_dict = {query_states_dict}, raise Fatal Error, the depends_on query may failed to start in 30s or exits/ends unexpectedly."
                    logger.error(error_msg)
                    query_depends_on_exception = TestException(
                        error_msg, ErrorCodes.QUERY_DEPENDS_ON_FAILED_TO_START
                    )
                    raise query_depends_on_exception
                else:
                    logger.info(
                        f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  query_id = {query_id}, depends_on = {depends_on} of query_id = {query_id} exists"
                    )
                time.sleep(1)  # for waiting the depends_on query ready.
            query_result_str = ""
            for command in commands:
                # if query_type is not None and query_type == 'rpk' and msg is not None:
                #     msg_str = json.dumps(msg)
                #     command = f'echo {msg_str} | {command}'
                query_result_str += str(self.exec_command(command))
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
                "query_id_type": query_id_type,
            }
        except BaseException as error:
            logger.error(f"exception, error = {error}")
            traceback.print_exc()
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
                "query_id_type": query_id_type,
            }
            logger.debug(
                "query_run_exec, db exception, none-cancel query_results: {}".format(
                    query_results
                )
            )
        finally:
            logger.debug(f"query_results = {query_results}")
            return query_results

    def exec_command(self, command, timeout=2):
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


class QueryExecuter(object):
    def __init__(
        self,
        config,
        child_conn,
        query_results_queue,
        alive,
        query_states_dict,
        logging_level="INFO",
    ):
        self._config = config
        self._child_conn = child_conn
        self._query_results_queue = query_results_queue
        self._alive = alive
        self._query_states_dict = query_states_dict
        self._logging_level = logging_level

    @property
    def config(self):
        return self._config

    @property
    def child_conn(self):
        return self._child_conn

    @property
    def query_results_queue(self):
        return self._query_results_queue

    @property
    def alive(self):
        return self._alive

    @alive.setter
    def alive(self, alive):
        self._alive = alive

    @property
    def query_states_dict(self):
        return self._query_states_dict

    @property
    def logging_level(self):
        return self._logging_level

    @classmethod
    def get_proton_client_config(cls, config):
        proton_setting = config.get("proton_setting")
        proton_cluster_query_node = config.get("proton_cluster_query_node")
        proton_server = None
        proton_server_native_port = ""
        if proton_setting is None or "cluster" not in proton_setting:
            proton_server = config.get("proton_server")
            proton_server_native_ports = config.get("proton_server_native_port")
            proton_server_native_ports = proton_server_native_ports.split(",")
            proton_server_native_port = proton_server_native_ports[
                0
            ]  # todo: get proton_server and port from statement
            proton_servers = None
        elif "cluster" in proton_setting and proton_cluster_query_node != "default":
            proton_servers = config.get("proton_servers")
            for item in proton_servers:
                node = item.get("node")
                if node == proton_cluster_query_node:
                    proton_server = item.get("host")
                    proton_server_native_port = item.get("port")
        else:  # if 'cluster' in proton_setting, go through the list and get the 1st node as default proton
            proton_servers = config.get("proton_servers")
            proton_server = proton_servers[0].get("host")
            proton_server_native_port = proton_servers[0].get("port")
        return (proton_servers, proton_server, proton_server_native_port)

    def query_exists_cluster(self, config, query_proc, client, retry_limit=300):
        proton_setting = config.get("proton_setting")
        test_suite_name = query_proc.get("test_suite_name")
        test_id = query_proc.get("test_id")
        query_id = query_proc.get("query_id")
        process = query_proc.get("process")
        retry = retry_limit
        try:
            if "cluster" not in proton_setting:
                while (
                    not QueryClientPy.query_id_exists_py(client, query_id)
                    and process.exitcode == None
                    and retry > 0
                ):  # process.exitcode is checked when another retry to identify if the query_run_py if already exist due to exception or other reasons.
                    time.sleep(0.1)
                    retry -= 1
            else:  # todo: consolidate the cluster query_id exists logic into a standalone function or query_exists()
                proton_servers = config.get("proton_servers")
                depends_on_exists = False
                depends_on_exists_flag = 0
                while (
                    not depends_on_exists and retry > 0
                ):  # todo: consolidate this cluster query exist into a cluster_query_exists function or query_exists()
                    for item in proton_servers:
                        proton_server = item.get("host")
                        proton_server_native_port = item.get("port")
                        logger.debug(
                            f"proton_server={proton_server}, proton_server_native_port = {proton_server_native_port}"
                        )
                        pyclient = Client(
                            host=proton_server,
                            port=proton_server_native_port,
                            send_receive_timeout=60,
                        )
                        if QueryClientPy.query_id_exists_py(pyclient, query_id):
                            depends_on_exists_flag += 1
                        pyclient.disconnect()
                    if depends_on_exists_flag > 0:
                        depends_on_exists = True
                    else:
                        depends_on_exists = False
                    time.sleep(0.1)
                    retry -= 1

            if retry > 0:
                logger.debug(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_exists_cluster check for query_id = {query_id}, found"
                )
                return True
            else:
                logger.debug(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_exists_cluster check for query_id = {query_id},  not found after {retry} times retry with interval = 0.1s."
                )
                return False

        except (BaseException, errors.ServerException) as error:
            error_msg = f"QUERY_EXISTS_CLUSTER_ERROR FATAL exception: proton_setting  = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id = {query_id}, error = {error}"
            logger.error(error_msg)
            traceback.print_exc()
            query_exists_cluster_exception = TestException(
                error_msg, ErrorCodes.QUERY_EXISTS_CLUSTER_ERROR
            )
            raise query_exists_cluster_exception

    def run(self):
        mp_mgr = None  # multiprocess manager, will be created when loading query_run_py process
        logger = mp.get_logger()
        pyclient = None
        try:
            time.sleep(1)
            query_procs = (
                []
            )  # a list of query_run_py processes started for queries of one case
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.formatter = formatter
            logger.addHandler(console_handler)
            if self.logging_level == "INFO":
                logger.setLevel(logging.INFO)
            elif self.logging_level == "DEBUG":
                logger.setLevel(logging.DEBUG)
            elif self.logging_level == "ERROR":
                logger.setLevel(logging.ERROR)
            elif self.logging_level == "WARNING":
                logger.setLevel(logging.WARNING)
            elif self.logging_level == "CRITICAL":
                logger.setLevel(logging.CRITICAL)
            else:
                logger.setLevel(logging.INFO)
            logger.debug(
                f"query_execute starts, logging_level = {self.logging_level}, logger.handlers = {logger.handlers}, query_states_dict = {self.query_states_dict}"
            )
            telemetry_shared_list = []  # telemetry list for query_run timing
            rest_setting = self.config.get("rest_setting")
            proton_setting = self.config.get("proton_setting")
            proton_server_container_name = self.config.get(
                "proton_server_container_name"
            )
            test_suite_name = self.config.get("test_suite_name")

            (
                proton_servers,
                proton_server,
                proton_server_native_port,
            ) = self.get_proton_client_config(self.config)
            proton_servers = [] if proton_servers is None else proton_servers
            proton_admin = self.config.get("proton_admin")
            proton_admin_name = proton_admin.get("name")
            proton_admin_password = proton_admin.get("password")
            settings = {"max_block_size": 100000}
            query_result_str = None
            tear_down = False
            query_run_count = 3000  # limit the query run count to 1000,
            query_result_list = []
            client = Client(
                host=proton_server,
                port=proton_server_native_port,
                send_receive_timeout=60,
            )  # create python client
            i = 0  # query
            auto_terminate_queries = []
            (
                test_id,
                query_id,
                query_type,
                query,
                query_start_time_str,
                query_end_time_str,
                message_recv,
            ) = (None, None, None, None, None, None, None)
            while (not tear_down) and query_run_count > 0 and self.alive.value:
                try:
                    query_proc = None
                    logger.debug(
                        f"query_execute: tear_down = {tear_down}, query_run_count = {query_run_count}, wait for message from test_suite_run......"
                    )
                    logger.debug(f"self.child_conn = {self.child_conn}")
                    message_recv = self.child_conn.recv()
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
                                proton_setting = proc.get("proton_setting")
                                test_suite_name = proc.get("test_suite_name")
                                test_id = proc.get("test_id")
                                wait = proc.get("wait")
                                query_id = proc.get("query_id")
                                exitcode = process.exitcode
                                terminate = proc.get("terminate")
                                query_end_timer = proc.get("query_end_timer")
                                if process.exitcode != None:
                                    i += 1
                                # else:
                                elif terminate in ("auto", "manual"):
                                    kill_query_exists_res = self.query_exists_cluster(
                                        self.config, proc, client
                                    )
                                    if (
                                        not kill_query_exists_res
                                    ):  # sleep for another "wait" and retry 10 times as final try.
                                        retry = 10
                                        if wait is not None:
                                            time.sleep(wait)
                                        kill_query_exists_res = (
                                            self.query_exists_cluster(
                                                self.config, proc, client, 10
                                            )
                                        )
                                    if not kill_query_exists_res:
                                        error_msg = f"QUERY_END_TIMER_DEPENDS_ON_NOT_EXIST, proton_server={proton_server}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_id check for query_end_timer failed, query_id = {query_id} for query_end_timer does not exist, the query may faild to start or exit by error."
                                        logger.error(error_msg)
                                        query_end_timer_depends_on_exception = TestException(
                                            error_msg,
                                            ErrorCodes.QUERY_END_TIMER_DEPENDS_ON_NOT_EXIST,
                                        )
                                        raise query_end_timer_depends_on_exception
                                    if query_end_timer != None:
                                        logger.debug(
                                            f"query_end_timer = {query_end_timer}, sleep {query_end_timer} seconds."
                                        )
                                        time.sleep(int(query_end_timer))
                                    logger.debug(
                                        f"query_end_timer sleep {query_end_timer} end start to call kill_query..."
                                    )
                                    if "cluster" not in proton_setting:
                                        QueryClientPy.kill_query(client, query_id)
                                        logger.debug(
                                            f"QueryClientPy.kill_query() was called, query_id={query_id}"
                                        )
                                    else:
                                        for item in proton_servers:
                                            proton_server = item.get("host")
                                            proton_server_native_port = item.get("port")
                                            logger.debug(
                                                f"proton_server={proton_server}, proton_server_native_port = {proton_server_native_port}"
                                            )
                                            if pyclient is not None:
                                                pyclient.disconnect()
                                                logger.debug(f"pyclient disconnect")
                                            pyclient = Client(
                                                host=proton_server,
                                                port=proton_server_native_port,
                                                send_receive_timeout=60,
                                            )
                                            QueryClientPy.kill_query(pyclient, query_id)
                                            logger.debug(
                                                f"QueryClientPy.kill_query() was called, query_id={query_id}"
                                            )
                                            pyclient.disconnect()
                            time.sleep(0.2)
                            retry = retry - 1
                        if (
                            retry == 0
                        ):  # terminate all the query process after retry timeout
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
                        self.child_conn.send(message_2_send)
                        query_run_count -= 1
                    else:
                        statement_2_run = json.loads(json.dumps(message_recv))
                        proton_setting = statement_2_run.get("proton_setting")
                        test_suite_name = statement_2_run.get("test_suite_name")
                        test_id = statement_2_run.get("test_id")
                        query_id = str(statement_2_run.get("query_id"))
                        query_client = statement_2_run.get("client")
                        query_type = statement_2_run.get("query_type")
                        user_name = statement_2_run.get("user")
                        password = statement_2_run.get("password")
                        terminate = statement_2_run.get("terminate")
                        run_mode = statement_2_run.get("run_mode")
                        logger.debug(
                            f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, query_states_dict = {self.query_states_dict}"
                        )
                        if (
                            run_mode == "process" or query_type == "stream"
                        ):  # only support query_state on a stream query started as a standlone process
                            self.query_states_dict[str(test_id)] = {
                                str(query_id): INIT_STATE
                            }  # query_states_dict: {test_id:{query_id:state}}
                        if terminate == "auto":
                            auto_terminate_queries.append(statement_2_run)
                        wait = statement_2_run.get("wait")
                        query = statement_2_run.get("query")
                        query_end_timer = statement_2_run.get("query_end_timer")
                        query_start_time_str = str(datetime.datetime.now())
                        query_end_time_str = str(datetime.datetime.now())
                        if run_mode == "process" or query_type == "stream":
                            mp_mgr = mp.Manager()
                            telemetry_shared_list = mp_mgr.list()
                            query_client_py_obj = QueryClientPy()
                            query_run_args = (
                                statement_2_run,
                                settings,
                                self.query_results_queue,
                                self.config,
                                None,
                                None,
                                self.query_states_dict,
                                telemetry_shared_list,
                                self.logging_level,
                            )
                            query_proc = mp.Process(
                                target=query_client_py_obj.run, args=query_run_args
                            )
                            query_procs.append(
                                {
                                    "process": query_proc,
                                    "terminate": terminate,
                                    "query_end_timer": query_end_timer,
                                    "query_id": query_id,
                                    "query": query,
                                    "proton_setting": proton_setting,
                                    "test_suite_name": test_suite_name,
                                    "test_id": test_id,
                                    "wait": wait,
                                }  # have to put append before start, otherwise exception when append shared list.
                            )  # put every query_run process into array for case_done check
                            query_proc.start()
                            logger.debug(
                                f"query_execute: start a proc for query_id = {query_id}, query_proc.pid = {query_proc.pid}"
                            )
                        else:
                            if query_client != None and query_client == "rest":
                                logger.debug(
                                    f"query_run_rest run local for query_id = {query_id}..."
                                )
                                query_client_rest_obj = QueryClientRest()
                                query_results = query_client_rest_obj.run(
                                    rest_setting, statement_2_run
                                )
                                logger.debug(
                                    f"query_id = {query_id}, query_run_rest is called"
                                )
                            elif query_client != None and query_client == "exec":
                                logger.debug(
                                    f"query_run_exec run local for query_id = {query_id}..."
                                )
                                query_client_exec_obj = QueryClientExec()
                                query_results = query_client_exec_obj.run(
                                    statement_2_run, self.config
                                )
                                logger.debug(
                                    f"query_id = {query_id}, query_run_exec is called"
                                )
                            else:
                                logger.debug(
                                    f"query_execute: query_run_py run local for query_id = {query_id}..."
                                )
                                query_client_py_obj = QueryClientPy()
                                logger.debug(
                                    f"query_id = {query_id}, to call query_run_py"
                                )
                                if user_name != None and password != None:
                                    statement_client = Client(
                                        host=proton_server,
                                        port=proton_server_native_port,
                                        user=user_name,
                                        password=password,
                                        send_receive_timeout=60,
                                    )
                                else:
                                    statement_client = client
                                query_results = query_client_py_obj.run(
                                    statement_2_run,
                                    settings,
                                    query_results_queue=None,
                                    config=self.config,
                                    pyclient=statement_client,
                                    logger=logger,
                                    query_states_dict=self.query_states_dict,
                                )
                                logger.debug(
                                    f"query_execute: query_results = {query_results}"
                                )
                                logger.debug(
                                    f"query_id = {query_id}, query_run_py is called"
                                )
                            message_2_send = json.dumps(query_results)
                            self.query_results_queue.put(message_2_send)
                            time.sleep(
                                0.05
                            )  # 0.2 originally, wait for the queue push completed
                        query_run_count = query_run_count - 1
                except (BaseException, errors.ServerException) as error:
                    logger.error(f"exception: error = {error}")
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
                        traceback.print_exc()
                    else:
                        query_results = {
                            "query_id": query_id,
                            "query": query,
                            "query_type": query_type,
                            "query_state": "exception",
                            "query_start": query_start_time_str,
                            "query_end": query_end_time_str,
                            "query_result": f"error_code:10000, error: {error}"
                            if error.code == None
                            else f"error_code:{error.code}, error: {error}",
                        }  # if it's not db excelption, send 10000 as error_code
                        traceback.print_exc()
                    message_2_send = json.dumps(query_results)
                    self.query_results_queue.put(message_2_send)
                    if message_recv == "test_steps_done":
                        message_2_send = "case_result_done"
                        # query_exe_queue.put(message_2_send)
                        self.child_conn.send(message_2_send)
                        query_run_count -= 1
                    query_run_count = query_run_count - 1
                finally:
                    if self.query_states_dict is not None:
                        self.query_states_dict[str(test_id)] = {
                            str(query_id): DONE_STATE
                        }
            if query_run_count == 0:
                logger.debug(
                    "Super, 3000 queries hit by a single test suite, we are in great time, by James @ Jan 10, 2022!"
                )
            print(
                f"query_execute: tear down msg recved, break while, start tear down and wait 3s for other processes shut down gracefully and terminiate all query_run_py processeses."
            )
            time.sleep(3)
            for proc in query_procs:
                process = proc.get("process")
                process.terminate()
                # process.join()
            logger.info(f"query_execute: all processes terminated.")
            all_query_end = False
            while not all_query_end and not tear_down and self.alive.value:
                print(
                    f"\nproton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name}, test_suite_name = {test_suite_name}, Query running status:"
                )
                for proc in query_procs:
                    query_id = proc.get("query_id")
                    query = proc.get("query")
                    process = proc.get("process")
                    query_process_exit_code = process.exitcode
                    if query_process_exit_code is None:
                        proc["status"] = "running"
                    else:
                        proc["status"] = "done"
                    print(
                        f"proton_setting = {proton_setting},proton_server_continaer_id = {proton_server_container_name}, test_suite_name = {test_suite_name}, query_id = {query_id}, query = {query}, exit_code = {query_process_exit_code}"
                    )
                for proc in query_procs:
                    status = proc.get("status")
                    if status == "running":
                        break
                    all_query_end = True
                time.sleep(30)
            for proc in query_procs:
                process = proc.get("process")
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
            self.child_conn.send("tear_down_done")
            logger.info(f"query_execute: tear_down completed and end")
            return
        except BaseException as error:
            logger.error(f"query_execute exception and end, error = {error}")
            return


class TestSuite(object):
    def __init__(
        self,
        config,
        test_suite_run_ctl_queue,
        test_suite_result_done_queue,
        test_suite_set_dict,
        query_states_dict,
    ):
        self._config = config
        self._test_suite_run_ctl_queue = test_suite_run_ctl_queue
        self._test_suite_result_done_queue = test_suite_result_done_queue
        self._test_suite_set_dict = test_suite_set_dict
        self._query_states_dict = query_states_dict

    @property
    def config(self):
        return self._config

    @property
    def test_suite_run_ctl_queue(self):
        return self._test_suite_run_ctl_queue

    @property
    def test_suite_result_done_queue(self):
        return self._test_suite_result_done_queue

    @property
    def test_suite_set_dict(self):
        return self._test_suite_set_dict

    @property
    def query_states_dict(self):
        return self._query_states_dict

    @classmethod
    def case_result_check(cls, test_set, order_check=False, logging_level="INFO"):
        try:
            expected_results = test_set.get("expected_results")
            statements_results = test_set.get("statements_results")
            proton_setting = test_set.get("proton_setting")
            test_suite_name = test_set.get("test_suite_name")
            test_id = test_set.get("test_id")
            statements_results_designed = (
                []
            )  # list for the query results for designated query_id
            for result in statements_results:  #
                # logger.debug(f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, statements_results = {statements_results}")
                if result != "aborted":
                    query_id_type = result.get("query_id_type")
                    if query_id_type == "designated":
                        statement_result = {
                            "query_id": result["query_id"],
                            "query_result": result["query_result"],
                            #'query_result_column_types': query_result_column_types,
                        }
                        statements_results_designed.append(statement_result)
                else:
                    test_set["statements_results_designed"] = "aborted"
            statements_results_designed_of_test_set = test_set.get(
                "statements_results_designed"
            )
            if statements_results_designed_of_test_set is None:
                test_set["statements_results_designed"] = statements_results_designed
            for (
                result
            ) in (
                statements_results
            ):  # check result, throw AssertException if result == "aborted"
                assert result != "aborted", f"statements_result = {result}"
            for i in range(len(expected_results)):  # for each query_results
                expected_result = expected_results[i].get("expected_results")
                expected_result_query_id = expected_results[i].get("query_id")
                query_results_dict = None
                for statement_results in statements_results:
                    assert statement_results != "aborted" and isinstance(
                        statement_results, dict
                    ), f"aborted or interruppted case"
                    statement_results_query_id = statement_results.get("query_id")
                    if statement_results_query_id == str(expected_result_query_id):
                        query_results_dict = statement_results
                assert (
                    query_results_dict != None
                )  # if no statement_results_query_id matches expected_result_query_id, case failed
                query_result = query_results_dict.get("query_result", {})
                # logging.info(f"\n test_run: expected_result = {expected_result}")
                # logging.info(f"\n test_run: query_result = {query_result}")
                query_result_column_types = query_results_dict.get(
                    "query_result_column_types", []
                )
                if (
                    query_result != "error_code:159"
                ):  # error_code:159 means Wait 2100 milliseconds for DDL operation timeout. the timeout is 2000 milliseconds, known issue of redpenda as external stream storage mode, skip result check.
                    assert type(expected_result) == type(
                        query_result
                    ), f"expected_result = {expected_result}, query_result = {query_result}"  # assert if the type of the query_result equals the type of expected_result
                    if isinstance(
                        expected_result, str
                    ):  # if the expected_result is a string lke "skip", "error_code:xxx"
                        if expected_result == "skip":
                            continue
                        else:
                            assert (
                                expected_result == query_result
                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                    elif isinstance(expected_result, dict):
                        for key in expected_result:
                            if expected_result[key] == "any_value":
                                pass
                            else:
                                assert (
                                    expected_result[key] == query_result[key]
                                ), f"expected_result = {expected_result}, query_result = {query_result}"
                    else:
                        if len(expected_result) == 0:
                            assert (
                                len(query_result) == 0
                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                        else:
                            assert len(expected_result) == len(
                                query_result
                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                            if (
                                order_check == False
                            ):  # when the order_check ==False, only check if the expected_reslt matches a query result but the sequence of the query result is not checked.
                                expected_result_check_arry = []
                                for i in range(len(expected_result)):
                                    expected_result_check_arry.append(0)
                                row_step = 0
                                for expected_result_row in expected_result:
                                    for query_result_row in query_result:
                                        expected_result_row_field_check_arry = []
                                        for i in range(
                                            # len(query_result_column_types) - 1 # query_result_column_types has a timestamp filed added by query_execute, so need to minus 1
                                            len(query_result_column_types)
                                        ):
                                            expected_result_row_field_check_arry.append(
                                                0
                                            )
                                        expected_result_row_check = 1
                                        for i in range(
                                            len(expected_result_row)
                                        ):  # for each filed of each row of each query_results
                                            expected_result_field = expected_result_row[
                                                i
                                            ]
                                            query_result_field = query_result_row[i]
                                            if (
                                                expected_result_field == "any_value"
                                            ):
                                                expected_result_row_field_check_arry[i] = 1
                                            elif (
                                                "array"
                                                in query_result_column_types[i][1]
                                                and "array_join"
                                                not in query_result_column_types[i][1]
                                            ):
                                                if (
                                                    expected_result_field
                                                    == query_result_field
                                                ):
                                                    expected_result_row_field_check_arry[
                                                        i
                                                    ] = 1
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                                else:
                                                    pass
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: not match")
                                            elif (
                                                "float"
                                                in query_result_column_types[i][1]
                                            ):
                                                if (
                                                    math.isclose(
                                                        float(expected_result_field),
                                                        float(query_result_field),
                                                        rel_tol=1e-2,
                                                    )
                                                    or math.isnan(
                                                        float(expected_result_field)
                                                    )
                                                    and math.isnan(
                                                        float(query_result_field)
                                                    )
                                                ):
                                                    expected_result_row_field_check_arry[
                                                        i
                                                    ] = 1
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                                else:
                                                    pass
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: not match")
                                            elif (
                                                "int" in query_result_column_types[i][1]
                                                and "tuple"
                                                not in query_result_column_types[i][1]
                                                and "map"
                                                not in query_result_column_types[i][1]
                                            ):
                                                if int(expected_result_field) == int(
                                                    query_result_field
                                                ):
                                                    expected_result_row_field_check_arry[
                                                        i
                                                    ] = 1
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                                else:
                                                    pass
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                            elif (
                                                "nullable"
                                                in query_result_column_types[i][1]
                                            ):
                                                if query_result_field == "None":
                                                    expected_result_row_field_check_arry[
                                                        i
                                                    ] = 1
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                                else:
                                                    pass
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")

                                            elif isinstance(expected_result_field, str):
                                                _match = 1
                                                _expected_field_itmes = (
                                                    expected_result_field.split(",")
                                                )
                                                _query_field_items = (
                                                    query_result_field.split(",")
                                                )
                                                for _expected_item, _result_item in zip(
                                                    _expected_field_itmes,
                                                    _query_field_items,
                                                ):
                                                    # logging.debug(f"_expected_item = {_expected_item}, _result_item = {_result_item}")
                                                    if (
                                                        _expected_item == "any_value"
                                                        or _expected_item
                                                        == _result_item
                                                    ):
                                                        _match *= 1
                                                    else:
                                                        _match *= 0
                                                if _match:
                                                    expected_result_row_field_check_arry[
                                                        i
                                                    ] = 1
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                                else:
                                                    pass
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: not match")
                                            else:
                                                if (
                                                    expected_result_field
                                                    == query_result_field
                                                ):
                                                    expected_result_row_field_check_arry[
                                                        i
                                                    ] = 1
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: match")
                                                else:
                                                    pass
                                                    # logging.debug(f"test_run: expected_result_field = {expected_result_field}, typeof expected_result = {type(expected_result_field)} query_result_field = {query_result_field} typeof query_result_field = {type(query_result_field)}")
                                                    # logging.debug("test_run: not match")
                                        # logging.debug(f"test_run: expected_result_row_field_check_arry = {expected_result_row_field_check_arry}")
                                        expected_result_row_check = 1
                                        for i in range(
                                            len(expected_result_row_field_check_arry)
                                        ):
                                            expected_result_row_check = (
                                                expected_result_row_check
                                                * expected_result_row_field_check_arry[
                                                    i
                                                ]
                                            )
                                        # logging.debug(f"test_run: expected_result_row_check = {expected_result_row_check}")
                                        if expected_result_row_check == 1:
                                            expected_result_check_arry[row_step] = 1
                                    assert (
                                        expected_result_check_arry[row_step] == 1
                                    ), f"expected_result = {expected_result}, query_result = {query_result}"
                                    row_step += 1
                            else:  # if order_check == True, assert the query result in the exact sequence of the expected result
                                for i in range(
                                    len(expected_result)
                                ):  # for each row of each query_results
                                    expected_result_row = expected_result[i]
                                    query_result_row = query_result[i]
                                    assert (
                                        len(expected_result_row)
                                        == len(query_result_row) - 1
                                    ), f"expected_result = {expected_result}, query_result = {query_result}"  # the timestamp field in query_result_row is artifically added and need to be excluded in the length
                                    for i in range(
                                        len(expected_result_row)
                                    ):  # for each filed of each row of each query_results
                                        expected_result_field = expected_result_row[i]
                                        query_result_field = query_result_row[i]
                                        if "array" in query_result_column_types[i][1]:
                                            assert (
                                                expected_result_field
                                                == query_result_field
                                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                                        elif "float" in query_result_column_types[i][1]:
                                            assert math.isclose(
                                                float(expected_result_field),
                                                float(query_result_field),
                                                rel_tol=1e-2,
                                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                                        elif "int" in query_result_column_types[i][1]:
                                            assert int(expected_result_field) == int(
                                                query_result_field
                                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                                        else:
                                            assert (
                                                expected_result_field
                                                == query_result_field
                                            ), f"expected_result = {expected_result}, query_result = {query_result}"
                else:
                    assert 1 == 1
            return True
        except AssertionError as ae:
            logger.info(f"assert error")
            traceback.print_exc()
            return False
        except BaseException as be:
            traceback.print_exc()
            logger.info(f"BaseException = {be}")
            return False

    def find_table_reset_in_table_schemas(self, table, table_schemas):
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

    def find_schema(self, table_name, table_schemas):
        if table_schemas == None:
            return None
        for table_schema in table_schemas:
            if table_name == table_schema.get("name"):
                return table_schema
        return None

    def test_suite_event_write(
        self,
        test_suite_event,
        test_suite_name,
        setting_config,
        test_suite_config,
        test_event_tag,
        version,
        timeplus_env,
        timeplus_stream_name,
    ):
        try:
            test_suite_info_tag = TestSuiteInfoTag(
                None, test_suite_name, setting_config, test_suite_config
            )
            test_suite_event_tag = TestSuiteEventTag(
                test_event_tag, test_suite_info_tag.value
            )
            test_suite_event_record = EventRecord(
                None, test_suite_event, test_suite_event_tag, version
            )
            test_suite_event_record.write(timeplus_env, timeplus_stream_name)
            return test_suite_event_record
        except BaseException as error:
            logger.error(f"timeplus event write exception: {error}")
            traceback.print_exc()
            return

    def test_suite_env_setup(self, client, config, test_suite_name, test_suite_config):
        logger = mp.get_logger()
        rest_setting = config.get("rest_setting")
        proton_setting = config.get("proton_setting")
        proton_create_stream_shards = config.get("proton_create_stream_shards")
        proton_create_stream_replicas = config.get("proton_create_stream_replicas")
        proton_server_container_name = config.get("proton_server_container_name")
        test_suite_env_setup_timeout = (
            DEFAULT_TEST_SUITE_TIMEOUT / 2
        )  # todo: test_suite_env_setup_timeout in config file
        test_suite_env_setup_timeout_hit = (
            threading.Event()
        )  # set the test_suite_timeout_hit flag as False and start a timer to set this flag
        timer = threading.Timer(
            test_suite_env_setup_timeout,
            timeout_flag,
            [
                test_suite_env_setup_timeout_hit,
                "TEST_SUITE_ENV_SETUP_TIMEOUT_ERROR FATAL and set the timeout treading event",
                f"test_suite_name = {test_suite_name}, proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name}",
            ],
        )
        timer.start()
        logger.debug(
            f"proton_setting = {proton_setting}, test_suite = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, test_suite_env_setup_timeout = {test_suite_env_setup_timeout}, timer started."
        )
        test_id = None
        try:
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
                logger.debug(f"table_name = {table_name}, reset = {reset}")
                table_type = table_schema.get("type")
                if reset != None and reset == "False":
                    pass
                else:
                    if table_type == "table":
                        drop_table_res = QueryClientRest.drop_table_if_exist_rest(
                            table_ddl_url, table_name
                        )
                        logger.debug(
                            f"QueryClientRest.drop_table_if_exist_rest({table_ddl_url}, {table_name}) = {drop_table_res}"
                        )
                        tables_setup.append(table_name)
                    elif table_type == "view":
                        drop_view_res = QueryClientPy.drop_if_exist_py(
                            client, "view", table_name
                        )
                        logger.debug(
                            f"QueryClientPy.drop_view_if_exist_py(clieent, {table_name}) = {drop_view_res}"
                        )
                        tables_setup.append(table_name)
            for table_schema in table_schemas:
                table_type = table_schema.get("type")
                table_name = table_schema.get("name")
                # if table_exist_py(client, table_name):
                if QueryClientRest.table_exist(table_ddl_url, table_name):
                    pass
                else:
                    if table_type == "table":
                        QueryClientRest.create_table_rest(config, table_schema)
                    elif table_type == "view":
                        QueryClientPy.create_view_if_not_exit_py(client, table_schema)
            setup = test_suite_config.get("setup")
            logger.debug(f"setup = {setup}")
            if setup is not None:
                test_id = "setup"  # if input_walk_through_rest is called in setup phase, set test_id = 'setup'
                setup_inputs = setup.get("inputs")
                setup_statements = setup.get(
                    "statements"
                )  # only support table rightnow todo: optimize logic
                if (
                    setup_statements != None
                    and not test_suite_env_setup_timeout_hit.is_set()
                ):
                    for statement_2_run in setup_statements:
                        settings = {"max_block_size": 100000}
                        query_id = statement_2_run.get("query_id")
                        if query_id is None:
                            query_id = str(uuid.uuid1())
                        statement_2_run["test_suite_name"] = test_suite_name
                        statement_2_run["test_id"] = test_id
                        statement_2_run["query_id"] = query_id
                        if test_suite_env_setup_timeout_hit.is_set():
                            logger.debug(
                                f"TEST_SUITE_ENV_SETUP_TIMEOUT_ERROR FATAL and break test_suite_evn_setup"
                            )
                            break
                        query_client_py = QueryClientPy()

                        query_results = query_client_py.run(
                            statement_2_run,
                            settings,
                            query_results_queue=None,
                            config=config,
                            pyclient=client,
                            logger=logger,
                        )
                        logger.debug(f"query_id = {query_id}, query_run_py is called")
                if (
                    setup_inputs is not None
                    and not test_suite_env_setup_timeout_hit.is_set()
                ):
                    logger.debug(f"input_walk_through_rest to be started.")
                    setup_input_res = self.input_walk_through_rest(
                        config, test_suite_name, test_id, setup_inputs, table_schemas
                    )
            timer.cancel()
        except BaseException as error:
            error_msg = f"TEST_SUITE_ENV_SETUP_ERROR FATAL exception: proton_setting = {proton_setting},test_suite_name = {test_suite_name}, test_id = {test_id}, proton_server_container_name = {proton_server_container_name},  error = {error}"
            logger.error(error_msg)  # todo: define private exception
            traceback.print_exc()
            timer.cancel()
            test_suite_env_setup_exception = TestException(
                error_msg, ErrorCodes.TEST_SUITE_ENV_SETUP_ERROR
            )
            raise test_suite_env_setup_exception
        return tables_setup

    def test_case_collect(self, test_suite, tests_2_run, test_ids_set, proton_setting):
        logger = mp.get_logger()
        test_ids_set_list = []
        try:
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
        except BaseException as error:
            error_msg = f"TEST_CASE_COLLECTION_ERROR, error = {error}"
            logger.error(error_msg)
            traceback.print_exc()
            test_case_collection_exception = TestException(
                error_msg, ErrorCodes.TEST_CASE_COLLECTION_ERROR
            )
            raise test_case_collection_exception
        return test_run_list

    def reset_tables_of_test_inputs(self, client, config, table_schemas, test_case):
        logger = mp.get_logger()
        rest_setting = config.get("rest_setting")
        proton_setting = config.get("proton_setting")
        test_suite_name = config.get("test_suite_name")
        proton_create_stream_shards = config.get("proton_create_stream_shards")
        proton_create_stream_replicas = config.get("proton_create_stream_replicas")
        table_ddl_url = rest_setting.get("table_ddl_url")
        steps = test_case.get("steps")
        proton_server_container_name = config.get("proton_server_container_name")
        tables_recreated = []
        try:
            for step in steps:
                if "inputs" in step:
                    inputs = step.get("inputs")
                    for (
                        input
                    ) in inputs:  # clean table data before each inputs walk through
                        res = None
                        logger.debug(f"input in inputs = {input}")
                        table = input.get("table_name")
                        is_table_reset = None
                        logger.debug(f"table of input in inputs = {table}")
                        is_table_reset = self.find_table_reset_in_table_schemas(
                            table, table_schemas
                        )
                        if (
                            is_table_reset != None and is_table_reset == False
                        ) or table in tables_recreated:
                            pass
                        else:
                            logger.debug(
                                f"proton_setting = {proton_setting},test_suite_name = {test_suite_name}, proton_server_container_name = {proton_server_container_name},  drop stream and re-create once case starts, table_ddl_url = {table_ddl_url}, table = {table}"
                            )
                            if QueryClientRest.table_exist(table_ddl_url, table):
                                QueryClientPy.drop_if_exist_py(client, "stream", table)
                            if table_schemas != None:
                                for table_schema in table_schemas:
                                    name = table_schema.get("name")
                                    tables_recreated.append(name)
                                    if name == table and QueryClientRest.table_exist(
                                        table_ddl_url, table
                                    ):
                                        QueryClientRest.create_table_rest(
                                            config, table_schema
                                        )
                                        tables_recreated.append(name)
                                    elif (
                                        name == table
                                        and not QueryClientRest.table_exist(
                                            table_ddl_url, table
                                        )
                                    ):
                                        QueryClientRest.create_table_rest(
                                            config, table_schema
                                        )
                                        tables_recreated.append(name)
            if len(tables_recreated) > 0:
                logger.debug(f"tables: {tables_recreated} are dropted and recreated.")
            return tables_recreated
        except BaseException as error:
            error_msg = f"RESET_STREAMS_OF_INPUTS_ERROR FATAL exception: proton_setting = {proton_setting}, proton_server_container_name = {proton_server_container_name},test_suite_name = {test_suite_name}, exception: {error}"
            logger.error(error_msg)
            traceback.print_exc()
            resset_streams_of_inputs_exception = TestException(
                error_msg, ErrorCodes.RESET_STREAMS_OF_INPUTS_ERROR
            )
            raise resset_streams_of_inputs_exception

    def query_walk_through(
        self,
        proton_setting,
        test_suite_name,
        test_id,
        statements,
        query_conn,
        test_suite_timeout_hit,
        alive,
    ):
        logger = mp.get_logger()
        statement_id_run = 0
        querys_results = []
        query_results_json_str = ""
        stream_query_id = "False"
        query_end_timer = 0
        max_wait = 0
        if test_suite_timeout_hit is not None and test_suite_timeout_hit.is_set():
            alive.value = False
        while (
            statement_id_run < len(statements) and not test_suite_timeout_hit.is_set()
        ):
            query_results = {}
            query_executed_msg = {}
            statement = statements[statement_id_run]
            client = statement.get("client")
            wait = statement.get("wait")
            if wait is None:
                wait = 0
            query_id = statement.get("query_id")
            query = statement.get("query")
            query_type = statement.get("query_type")
            run_mode = statement.get("run_mode")
            terminate = statement.get("terminate")
            statement["proton_setting"] = proton_setting
            statement["test_id"] = test_id
            statement["test_suite_name"] = test_suite_name
            query_id_type = statement.get("query_id_type")
            if (
                query_id is None and query_id_type is None
            ):  # the query_id_type is only set when 1st run of query_walk_throug, otherwise it will be set wrong during failed case retry
                query_id = str(uuid.uuid1())
                statement["query_id"] = query_id
                statement["query_id_type"] = "non-designated"
            else:
                if query_id is not None and query_id_type is None:
                    statement[
                        "query_id_type"
                    ] = "designated"  # mark the query_id_type, when query_result_check, only show the "designated" satement result to make the troule shooting easy
                elif query_id_type == "non-designated":
                    query_id = str(uuid.uuid1())
            if query_type == "stream" and terminate == None:
                statement[
                    "terminate"
                ] = "auto"  # for stream query, by default auto-terminate
            if query_end_timer == None:
                query_end_timer = 0
            if query_type == "stream" or run_mode == "process":
                if max_wait < int(wait):
                    max_wait = int(wait)
            else:
                max_wait += wait
            # query_exe_queue.put(statement)
            if test_suite_timeout_hit is not None and test_suite_timeout_hit.is_set():
                alive.value = False
                break
            query_conn.send(statement)
            logger.debug(
                # f"query_walk_through: statement query_id = {query_id} was pushed into query_exe_queue."
                f"query_walk_through: statement query_id = {query_id}, query = {query} was send to query_execute."
            )
            statement_id_run += 1
            # time.sleep(1) # wait the query_execute execute the stream command

        logger.debug(f"query_walk_through: end... stream_query_id = {stream_query_id}")
        return max_wait

    def input_walk_through_rest(
        self,
        config,
        test_suite_name,
        test_id,
        inputs,
        table_schemas,
        test_suite_timeout_hit=None,
        alive=None,
        query_states_dict=None,
        wait_before_inputs=1,  # todo: remove all the sleep
        sleep_after_inputs=1.5,  # todo: remove all the sleep (current stable set wait_before_inputs=1, sleep_after_inputs=1.5)
    ):
        logger = mp.get_logger()
        rest_setting = config.get("rest_setting")
        proton_setting = config.get("proton_setting")
        # logger.debug(f"rest_setting = {rest_setting}, table_schemas = {table_schemas}")
        wait_before_inputs = wait_before_inputs  # the seconds sleep before inputs starts to ensure the query is run on proton.
        sleep_after_inputs = sleep_after_inputs  # the seconds sleep after evary inputs of a case to ensure the stream query result was emmited by proton and received by the query execute
        time.sleep(wait_before_inputs)
        input_url = rest_setting.get("ingest_url")
        inputs_record = []
        (
            proton_servers,
            proton_server,
            proton_server_native_port,
        ) = QueryExecuter.get_proton_client_config(config)
        client = Client(
            host=proton_server, port=proton_server_native_port, send_receive_timeout=60
        )  # create python client
        try:
            for batch in inputs:
                if (
                    test_suite_timeout_hit is not None
                    and test_suite_timeout_hit.is_set()
                ):
                    alive.value = False
                    break
                table_name = batch.get("table_name")
                depends_on = batch.get("depends_on")
                table_schema = self.find_schema(table_name, table_schemas)
                logger.debug(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, input_walk_through_rest: table_schema = {table_schema}"
                )
                batch_sleep_before_input = batch.get("sleep")
                if batch_sleep_before_input != None:
                    logger.info(f"sleep {batch_sleep_before_input} before input")
                    time.sleep(int(batch_sleep_before_input))
                input_batch_record = QueryClientRest.input_batch_rest(
                    config,
                    test_suite_name,
                    test_id,
                    batch,
                    table_schema,
                    query_states_dict,
                )
                inputs_record.append(input_batch_record)
                for act in ACTS_IN_FINAL:
                    scan_and_killres = QueryClientPy.scan_statement_and_act(
                        batch, act, client
                    )
            if isinstance(sleep_after_inputs, int) and (
                test_suite_timeout_hit is None or not test_suite_timeout_hit.is_set()
            ):
                time.sleep(sleep_after_inputs)
        except BaseException as error:
            error_msg = f"INPUT_ERROR FATAL exception: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, error = {error}"
            logger.error(error_msg)
            traceback.print_exc()
            if client is not None:
                client.disconnect()
            input_exception = TestException(error_msg, ErrorCodes.INPUT_ERROR)
            raise input_exception
        if client is not None:
            client.disconnect()
        return inputs_record

    def run(
        self,
    ):
        logger = mp.get_logger()
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.formatter = formatter
        logging_level = self.test_suite_set_dict.get("logging_level")
        logger.addHandler(console_handler)
        logger.setLevel(logging_level)
        proton_setting = self.config.get("proton_setting")
        test_suite_name = self.test_suite_set_dict.get("test_suite_name")
        proton_server_container_name = self.config.get("proton_server_container_name")
        self.config[
            "test_suite_name"
        ] = test_suite_name  # test_suite_run is started to be run in a standalone process, set the test_suite_name in config, todo: logic for test_suite_run is not in multiple process
        test_suite = self.test_suite_set_dict.get("test_suite")
        test_retry = self.config.get("test_retry")
        test_suite_timeout = test_suite.get("test_suite_timeout")
        if (
            test_suite_timeout is None
        ):  # if test_suite_timeout is not set in test_suite, use the test_suite_timeout in config, if test_suite_timeout is not set in config, use the default value
            test_suite_timeout = self.config.get("test_suite_timeout")
            if test_suite_timeout is None:
                test_suite_timeout = DEFAULT_TEST_SUITE_TIMEOUT
        test_event_tag = self.config.get("test_event_tag")
        test_event_version = self.config.get("timeplus_event_version")
        timeplus_event_stream = self.config.get("timeplus_event_stream")
        if test_suite != None and len(test_suite) != 0:
            test_suite_config = test_suite.get("test_suite_config")
        else:
            test_suite_config = {}
        client = None
        test_run_list_len_total = 0
        api_key = os.environ.get("TIMEPLUS_API_KEY")
        api_address = os.environ.get("TIMEPLUS_ADDRESS", "")
        work_space = os.environ.get("TIMEPLUS_WORKSPACE")
        if work_space is not None and work_space != "":
            api_address = api_address + "/" + work_space
        timeplus_env = None
        if (
            test_event_tag is not None
            and api_key is not None
            and api_address is not None
        ):
            try:
                timeplus_env = Environment().address(api_address).apikey(api_key)
                event_type = "test_suite_event"
                event_detailed_type = "status"
                event_details = "start"
                test_suite_event_start = Event.create(
                    event_type, event_detailed_type, event_details
                )
                test_suite_event = self.test_suite_event_write(
                    test_suite_event_start,
                    test_suite_name,
                    self.config,
                    test_suite_config,
                    test_event_tag,
                    test_event_version,
                    timeplus_env,
                    timeplus_event_stream,
                )
            except BaseException as error:
                logger.error(f"timeplus event write exception: {error}")
                traceback.print_exc()
        rest_setting = self.config.get("rest_setting")
        test_case_timeout = self.config.get("test_case_timeout")
        if test_case_timeout is None or test_case_timeout == DEFAULT_CASE_TIMEOUT:
            test_case_timeout = test_suite.get("test_case_timeout")
            if test_case_timeout is None:
                test_case_timeout = DEFAULT_CASE_TIMEOUT
        rest_setting = self.config.get("rest_setting")
        test_ids_set = os.getenv("PROTON_TEST_IDS", None)
        test_run_list = []
        test_run_list_len = 0
        test_run_id_list = []
        test_sets_2_run = []
        table_schemas = []
        test_id_run = 0
        if test_suite != None and len(test_suite) != 0:
            if test_suite_config != None:
                table_schemas = test_suite_config.get("table_schemas")
            tests = test_suite.get("tests")
            tests_2_run = test_suite_config.get("tests_2_run")
            logger.debug("test_case_collect is to be started......")
            test_run_list = self.test_case_collect(
                test_suite, tests_2_run, test_ids_set, proton_setting
            )
            test_run_list_len = len(test_run_list)
            # client = None
        test_suite_result_summary = {}
        if test_run_list_len == 0:
            print(
                f"proton_setting = {proton_setting}, test_suite = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, test_retry = {test_retry}, test_run_list_lan = 0, test_running bypassed......"
            )
            logger.info(
                f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_run_list = {test_run_list}, 0 case collected, bypass."
            )
            test_suite_result_summary = {
                "test_suite_name": test_suite_name,
                "test_run_list_len": 0,
                "test_sets": [],
                "test_list": [],
                "proton_setting": proton_setting,
                # "test_suite_run_status": [],
                "test_suite_result": False,
                "test_suite_case_run_duration": 0,
                "test_suite_run_duration": 0,
                "test_suite_passed_total": 0,
                "proton_server_container_name": proton_server_container_name,
            }
            # test_run_list_len_total = 0
            # test_sets = []
            self.test_suite_run_ctl_queue.get()
            self.test_suite_run_ctl_queue.task_done()
            self.test_suite_result_done_queue.put(test_suite_result_summary)
            self.test_suite_result_done_queue.join()
        else:
            print(
                f"proton_setting = {proton_setting}, test_suite = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, test_retry = {test_retry}, test_suite_timeout = {test_suite_timeout},test_event_tag = {test_event_tag}, query_states_dict = {self.query_states_dict}, test_running starts......"
            )
            query_results_queue = self.test_suite_set_dict.get("query_results_queue")
            (
                query_conn,  # query_exe_parent_conn
                q_exec_client_conn,  # query_exe_child_conn
            ) = mp.Pipe(
                True
            )  # create the pipe for inter-process conn of each test_suite_run and query_execute pair, control path
            alive = self.test_suite_set_dict.get("alive")
            query_executer_obj = QueryExecuter(
                self.config,
                q_exec_client_conn,
                query_results_queue,
                alive,
                self.query_states_dict,
                logging_level,
            )  # under construction
            query_exe_client = mp.Process(
                target=query_executer_obj.run,
                args=(),
            )  # Create query_exe_client process
            query_results_queue = self.test_suite_set_dict.get("query_results_queue")
            logger.debug(f"alive.value = {alive.value}")
            query_exe_client.start()  # start the query execute process
            logger.debug(
                f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, query_exe_client started: {query_exe_client}."
            )
            to_start_at = self.config.get(
                "to_start_at"
            )  # calculate wait time to launch
            test_suite_launch_interval = self.config.get("test_suite_launch_interval")
            if test_suite_launch_interval is None:
                test_suite_launch_interval = TEST_SUITE_LAUNCH_INTERVAL
            logger.debug(
                f"watch: proton_setting = {proton_setting}, test_suite = {test_suite_name}, proton_server_container_name = {proton_server_container_name},to_start_at = {to_start_at}"
            )
            if (
                to_start_at is not None
            ):  # if no last_test_suite_lanuched_at in config, that means the 1st test suite running on the env
                to_start_at = datetime.datetime.strptime(to_start_at, TIME_STR_FORMAT)
                sleep_time = to_start_at - datetime.datetime.now()
                time.sleep(sleep_time.total_seconds())
                logger.debug(
                    f"proton_setting = {proton_setting}, test_suite = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, waited for sleep_time = {sleep_time} seconds to launch."
                )
            test_suite_start = datetime.datetime.now()
            test_suite_case_run_end = datetime.datetime.now()
            test_suite_end = datetime.datetime.now()
            test_suite_case_run_duration = test_suite_case_run_end - test_suite_start
            test_suite_run_duration = test_suite_end - test_suite_start
            test_suite_passed_total = 0
            test_id = ""  # initialize test_id
            test_suite_timeout_hit = (
                threading.Event()
            )  # set the test_suite_timeout_hit flag as False and start a timer to set this flag
            timer = threading.Timer(
                test_suite_timeout,
                timeout_flag,
                [
                    test_suite_timeout_hit,
                    "TEST_SUITE_TIME_OUT_ERROR FATAL and set the timeout treading event",
                    f"test_suite_name = {test_suite_name}, proton_setting = {proton_setting}",
                ],
            )
            timer.start()
            logger.debug(
                f"proton_setting = {proton_setting}, test_suite = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, test_suite_timeout = {test_suite_timeout}, timer started."
            )
            proton_cluster_query_node = self.config.get("proton_cluster_query_node")
            proton_server = None
            proton_server_native_port = None
            if "cluster" not in proton_setting:
                proton_server = self.config.get("proton_server")
                proton_server_native_ports = self.config.get(
                    "proton_server_native_port"
                )
                proton_server_native_ports = proton_server_native_ports.split(",")
                proton_server_native_port = proton_server_native_ports[
                    0
                ]  # todo: get proton_server and port from statement
            elif "cluster" in proton_setting and proton_cluster_query_node != "default":
                proton_servers = self.config.get("proton_servers")
                for item in proton_servers:
                    node = item.get("node")
                    if node == proton_cluster_query_node:
                        proton_server = item.get("host")
                        proton_server_native_port = item.get("port")
            else:  # if 'cluster' in proton_setting, go through the list and get the 1st node as default proton
                proton_servers = self.config.get("proton_servers")
                proton_server = proton_servers[0].get("host")
                proton_server_native_port = proton_servers[0].get("port")
            logger.debug(
                f"proton_server = {proton_server}, proton_server_native_port = {proton_server_native_port}"
            )
            for test in test_run_list:
                test_id = test.get("id")
                test_name = test.get("name")
                steps = test.get("steps")
                expected_results = test.get("expected_results")
                # test_suite_run_status.append({"test_id": test_id, "status":"aborted"}) #list for test suite running status, aborted or done
                test_run_id_list.append(test_id)
                test_sets_2_run.append(
                    {
                        "proton_setting": proton_setting,
                        "test_suite_name": test_suite_name,
                        "test_id_run": 0,
                        "test_id": test_id,
                        "test_name": test_name,
                        "steps": steps,
                        "expected_results": expected_results,
                        "statements_results": ["aborted"],
                        "status": "aborted",
                        "test_result": "",
                        "test_case_duration": 0,
                        "case_retried": 0,
                    }
                )
            try:
                client = Client(
                    host=proton_server,
                    port=proton_server_native_port,
                    send_receive_timeout=60,
                )
                if test_suite_config != None:
                    logger.debug(f"test_suite_env_setup is to be started......")
                    tables_setup = self.test_suite_env_setup(
                        client, self.config, test_suite_name, test_suite_config
                    )  # setup env for test suite running
                    time.sleep(
                        1
                    )  # sleep after test_suite_env_setup, for some streams created during test_suite_evn_setup, sleep to wait the stream setting up done
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
                i = 0  # counter for test_run_list
                j = 0  # counter for retry_cases
                logger.debug(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_suite_timeout_hit.is_set() = {test_suite_timeout_hit.is_set()}"
                )
                if test_suite_timeout_hit.is_set():
                    error_msg = f"TEST_SUITE_TIMEOUT_ERROR FATAL exception: proton_setting={proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, test_suite_timeout = {test_suite_timeout} hit"
                    logger.error(error_msg)
                    test_suite_timeout_exception = TestException(
                        error_msg, ErrorCodes.TEST_SUITE_TIMEOUT_ERROR
                    )
                    raise test_suite_timeout_exception
                retry_cases = []  # record the falied case
                retry_times = CASE_RETRY_TIMES  # hard code firstly and refine later to make it a parameter of ci_runner.py
                retry_cases_num = 0  # retry_case_num will be set when the 1st round of the test_suite execution ends
                fatal_retry_times = CASE_RETRY_TIMES  # count for retried fatal
                case_retry_flag = False
                while (
                    i < len(test_run_list)
                    or (
                        j < retry_cases_num
                        and retry_times > 0
                        and retry_cases_num <= CASE_RETRY_UP_LIMIT
                    )
                    and test_retry == "True"
                ) and not test_suite_timeout_hit.is_set():  # only case retry when failed case number less than case_retry_up_limit
                    test_case_start = datetime.datetime.now()
                    test_case_end = datetime.datetime.now()
                    test_case_duration = test_case_end - test_case_start
                    recovered_test_ids = (
                        []
                    )  # record the test id of the retry success case
                    fatal_exception_flag = False
                    if not case_retry_flag:
                        test_case = test_run_list[i]
                    else:
                        test_case = retry_cases[j]
                        logger.info(
                            f'retry case: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_case["id"]}, test_case = {test_case}'
                        )
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
                    logger.debug("reset_table_of_test_inputs to be starts...")
                    tables_recreated = self.reset_tables_of_test_inputs(
                        client, self.config, table_schemas, test_case
                    )
                    logger.info(
                        f"tables: {tables_recreated} are dropted and recreated."
                    )
                    logger.info(
                        f"proton_setting = {proton_setting}, test_id_run = {test_id_run}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id} starts......"
                    )
                    wait_before_inputs = 0
                    for step in steps:
                        statements_id = 0
                        inputs_id = 0
                        if "statements" in step:
                            step_statements = step.get("statements")
                            query_walk_through_res = self.query_walk_through(
                                proton_setting,
                                test_suite_name,
                                test_id,
                                step_statements,
                                query_conn,
                                test_suite_timeout_hit,
                                alive,
                            )  # walk through statements, todo: optimize the statement context building up logic
                            wait_before_inputs = query_walk_through_res  # get the max_wai in query_walk_through
                            logger.info(
                                f"proton_setting = {proton_setting}, test_suite_run: {test_id_run}, test_suite_name = {test_suite_name}, case_retry_flag = {case_retry_flag}, test_id = {test_id}, step{step_id}.statements{statements_id}, done..."
                            )
                            statements_id += 1
                        elif "inputs" in step:
                            time.sleep(
                                wait_before_inputs
                            )  # auto wait the max_wait of the query_execute
                            inputs = step.get("inputs")
                            try:
                                inputs_record = self.input_walk_through_rest(
                                    self.config,
                                    test_suite_name,
                                    test_id,
                                    inputs,
                                    table_schemas,
                                    test_suite_timeout_hit,
                                    alive,
                                    self.query_states_dict,
                                )  # inputs walk through rest_client
                                logger.info(
                                    f"proton_setting = {proton_setting}, test_id_run = {test_id_run}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag},  test_id = {test_id} input_walk_through done"
                                )
                                # time.sleep(0.5) #wait for the data inputs done.
                            except BaseException as error:
                                error_msg = f"INPUT_ERROR FATAL exception: proton_setting = {proton_setting}, test_id_run = {test_id_run}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag},  test_id = {test_id}, error = {error}"
                                logger.error(error_msg)
                                traceback.print_exc()
                                if not fatal_exception_flag:
                                    fatal_exception_flag = True
                                if fatal_retry_times <= 0:
                                    input_exception = TestException(
                                        error_msg, ErrorCodes.INPUT_ERROR
                                    )
                                    raise input_exception
                        step_id += 1
                    query_conn.send("test_steps_done")
                    logger.debug("test_steps_done sent to query_execute")
                    message_recv = (
                        query_conn.recv()
                    )  # wait the query_execute to send "case_result_done" to indicate all the statements in pipe are consumed.
                    assert message_recv == "case_result_done"
                    while (
                        not query_results_queue.empty()
                    ):  # collect all the query_results from queue after "case_result_done" received
                        time.sleep(0.2)
                        message_recv = query_results_queue.get()
                        query_results = json.loads(message_recv)
                        query_id = query_results.get("query_id")
                        query = query_results.get("query")
                        query_type = query_results.get("query_type")
                        # logger.info(f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id}, query_id = {query_id}, query_type = {query_type}, query = {query}, query_result recved in test_suite_run")
                        query_state = query_results.get("query_state")
                        if query_state is not None and (
                            query_state == "crash" or query_state == "fatal"
                        ):  # when Connection related error happens, it will be set in the query_state of the query results
                            error = query_results.get("error")
                            for (
                                item
                            ) in (
                                test_run_list
                            ):  # when crash or fatal error, put the error msg into statements result
                                # item_id = test.get("id")
                                if (
                                    case_retry_flag
                                ):  # when crash or fatal error happens during retry, need to set the 'status' to 'aborted'
                                    item["status"] = "aborted"
                            if query_state == "crash":
                                error_msg = f"QUERY_CRASH_ERROR CRASH exception: proton_setting = {proton_setting},proton_server_container_name = {proton_server_container_name}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id}, test_suite_run, proton crash happens = {error}, raise Exception"
                                logger.error(error_msg)
                                query_crash_exception = TestException(
                                    error_msg, ErrorCodes.QUERY_CRASH_ERROR
                                )
                                raise query_crash_exception
                            else:
                                error_msg = f"QUERY_FATAL_ERROR FATAL exception: proton_setting = {proton_setting},proton_server_container_name = {proton_server_container_name}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id}, test_suite_run, proton fatal happens = {error}, raise Exception"
                                logger.error(error_msg)
                                query_fatal_exception = TestException(
                                    error_msg, ErrorCodes.QUERY_FATAL_ERROR
                                )
                                fatal_exception_flag = True
                                if (
                                    fatal_retry_times <= 0
                                ):  # when retry on fatal hit fatal_retry_times, raise except to stop test_suite_run
                                    logger.error(error_msg)
                                    raise query_fatal_exception
                        statements_results.append(query_results)
                    if (
                        fatal_exception_flag
                    ):  # -1 after retry case with FATAL but not -1 after a
                        fatal_retry_times -= 1
                    test_case_end = datetime.datetime.now()
                    test_case_duration = test_case_end - test_case_start
                    for (
                        test
                    ) in (
                        test_sets_2_run
                    ):  # update case status and result, todo: change the dict structure of the test_sets_2_run to use test_id as a key to simplify the case locating for result update
                        test_2_run_id = test.get("test_id")
                        if test_2_run_id == test_id:
                            test["test_id_run"] = test_id_run
                            test["expected_results"] = expected_results
                            test["statements_results"] = statements_results
                            test["test_case_duration"] = test_case_duration.seconds
                            if fatal_exception_flag:
                                test["status"] = "aborted"
                                if case_retry_flag:
                                    test["case_retried"] = int(test["case_retried"]) + 1
                            else:
                                if case_retry_flag:
                                    test["status"] = "retried"  # set status to retried
                                    test["case_retried"] = int(test["case_retried"]) + 1
                                else:
                                    test["status"] = "done"  # set status to done
                            case_result = self.case_result_check(
                                test
                            )  # check test case result
                            test["test_result"] = case_result
                            retry_case_ids = []
                            if (
                                not case_result
                            ):  # todo: optimize the logic here to make it more simple
                                if len(retry_cases) == 0:
                                    retry_cases.append(test_case)
                                else:
                                    for case in retry_cases:
                                        case_id = case.get("id")
                                        retry_case_ids.append(str(case_id))
                                    if str(test_id) not in retry_case_ids:
                                        retry_cases.append(test_case)
                                logger.info(
                                    f"case failed: proton_setting = {proton_setting}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id}"
                                )
                                logger.debug(
                                    f"case failed: proton_setting={proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, expected_results = {expected_results}, statements_results = {statements_results}"
                                )
                            elif (
                                case_retry_flag
                            ):  # during retry, if the case passed, pop from retry_cases
                                test_suite_passed_total += 1
                                recovered_test_ids.append(str(test_id))
                                logger.info(
                                    f"case retry passed: proton_setting = {proton_setting}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id}"
                                )
                            else:
                                test_suite_passed_total += 1
                                logger.info(
                                    f"case passed: proton_setting = {proton_setting}, test_suite_name = {test_suite_name},case_retry_flag = {case_retry_flag}, test_id = {test_id}"
                                )
                    if (
                        not case_retry_flag
                    ):  # when the 1st round of test suite execution, i increase, when retry j increase, test_id_run records the run sequence
                        i += 1
                        if i == len(test_run_list):
                            retry_cases_num = len(
                                retry_cases
                            )  # when the 1st round of test suite execution ends, set retry_case_num
                            if retry_cases_num > 0:
                                case_retry_flag = (
                                    True  # if retry_cases_num > 0, set case_retry_flag
                                )
                                logger.info(
                                    f"First run of the test suite done: proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, there are {retry_cases_num} retry_cases, set case_retry_flag = {case_retry_flag}"
                                )
                                time.sleep(5)  # wait or 5 second to start the retry
                    else:
                        j += 1
                        if j == len(retry_cases):
                            # retry_cases_num = len(retry_cases) #when the 1st round of test suite execution ends, set retry_case_num
                            retry_times -= 1
                            j = 0  # reset counter of retry_cases to 0, if retry_times > 1 and
                            retry_cases_copy = []
                            for case in retry_cases:
                                test_id = case[
                                    "id"
                                ]  # Be careful no test_id field in case, only id
                                if str(test_id) not in recovered_test_ids:
                                    retry_cases_copy.append(test_case)
                            retry_cases = retry_cases_copy  # reset the retry_cases and remove the cases passed during retry.
                            retry_cases_num = len(retry_cases)
                            time.sleep(
                                5
                            )  # wait for 5 secs to start another round retry
                    test_id_run += 1
                    if test_suite_timeout_hit.is_set():
                        error_msg = f"TEST_SUITE_TIMEOUT_ERROR FATAL exception: proton_setting={proton_setting}, test_suite_name = {test_suite_name}, test_id = {test_id}, test_suite_timeout = {test_suite_timeout} hit"
                        logger.error(error_msg)
                        test_suite_timeout_exception = TestException(
                            error_msg, ErrorCodes.TEST_SUITE_TIMEOUT_ERROR
                        )
                        raise test_suite_timeout_exception
                test_suite_result_summary = {
                    "test_suite_name": test_suite_name,
                    "test_run_list_len": test_run_list_len,
                    "test_sets": test_sets_2_run,
                    "test_list": test_run_list,
                    "proton_setting": proton_setting,
                    # "test_suite_run_status": test_suite_run_status,
                    "test_suite_result": False,
                    "test_suite_passed_total": test_suite_passed_total,
                    "proton_server_container_name": proton_server_container_name,
                }
            except BaseException as error:
                logger.error(f"test_suite_run, exception: {error}, ")
                traceback.print_exc()
                test_suite_result_summary = {
                    "test_suite_name": test_suite_name,
                    "test_run_list_len": test_run_list_len,
                    "test_sets": test_sets_2_run,
                    "test_list": test_run_list,
                    "proton_setting": proton_setting,
                    # "test_suite_run_status": test_suite_run_status,
                    "test_suite_result": False,
                    "test_suite_passed_total": test_suite_passed_total,
                    "proton_server_container_name": proton_server_container_name,
                }
                if (
                    test_event_tag is not None
                    and api_key is not None
                    and api_address is not None
                ):
                    try:
                        event_type = "test_suite_event"
                        event_detailed_type = "exception"
                        formatted_lines = traceback.format_exc()
                        event_details = {
                            "error": f"{error}",
                            "traceback": formatted_lines,
                        }
                        test_suite_event_exception = Event.create(
                            event_type, event_detailed_type, event_details
                        )
                        test_suite_event = self.test_suite_event_write(
                            test_suite_event_exception,
                            test_suite_name,
                            self.config,
                            test_suite_config,
                            test_event_tag,
                            test_event_version,
                            timeplus_env,
                            timeplus_event_stream,
                        )
                        print(f"test_suite_event = {test_suite_event}")
                    except BaseException as error:
                        logger.error(f"timeplus event write exception: {error}")
                        traceback.print_exc()
            finally:
                test_suite_case_run_end = datetime.datetime.now()
                test_suite_case_run_duration = (
                    test_suite_case_run_end - test_suite_start
                )
                test_suite_run_duration = test_suite_case_run_duration
                test_suite_result_summary[
                    "test_suite_case_run_duration"
                ] = test_suite_case_run_duration.seconds
                test_suite_result_summary[
                    "test_suite_run_duration"
                ] = (
                    test_suite_run_duration.seconds
                )  # set test_suite_run_duration same as test_suite_case_run_duration  when updateing test_suite_case_run_duration
                self.test_suite_run_ctl_queue.get()
                self.test_suite_run_ctl_queue.task_done()
            print(
                f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name} running ends"
            )
            print(
                f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, proton_server_container_name = {proton_server_container_name}, test_run_list_len = {test_run_list_len}, test_suite_passed_total = {test_suite_passed_total}, test_suite_case_run_duration = {test_suite_case_run_duration.seconds} seconds, test_suite_run_duration = {test_suite_run_duration.seconds} seconds, test_suite_run_status: "
            )
            for test_set in test_sets_2_run:
                print(
                    f'test_id = {test_set["test_id"]}, test_status = {test_set["status"]},case_retried = {test_set["case_retried"]}, test_result = {test_set["test_result"]}, test_case_duration = {test_set["test_case_duration"]} seconds'
                )
            if (
                test_event_tag is not None
                and api_key is not None
                and api_address is not None
            ):
                try:
                    test_suite_result_running_summary = {
                        "test_run_list_len": test_run_list_len,
                        "test_suite_passed_total": test_suite_passed_total,
                        "test_suite_case_run_duration": test_suite_case_run_duration.seconds,
                    }
                    test_case_run_summary_list = []
                    test_suite_result_flag = 1
                    for test_set in test_sets_2_run:
                        test_case_run_summary = {
                            "test_id": test_set["test_id"],
                            "test_status": test_set["status"],
                            "case_retried": test_set["case_retried"],
                            "test_result": test_set["test_result"],
                            "test_case_duration": test_set["test_case_duration"],
                        }
                        if test_set["test_result"] is not True:
                            test_suite_result_flag = test_suite_result_flag * 0
                        test_case_run_summary_list.append(test_case_run_summary)
                        # logger.info(f'test_id = {test_set["test_id"]}, test_status = {test_set["status"]},case_retried = {test_set["case_retried"]}, test_result = {test_set["test_result"]}, test_case_duration = {test_set["test_case_duration"]} seconds')
                    if test_suite_result_flag:
                        test_suite_result = {
                            "test_suite_result": "success",
                            "detailed_summary": {
                                **test_suite_result_running_summary,
                                **{"test_case_results": test_case_run_summary_list},
                            },
                        }
                    else:
                        test_suite_result = {
                            "test_suite_result": "failed",
                            "detailed_summary": {
                                **test_suite_result_running_summary,
                                **{"test_case_results": test_case_run_summary_list},
                            },
                        }
                    event_type = "test_suite_event"
                    event_detailed_type = "status"
                    event_details = "end"
                    test_suite_event_end = Event.create(
                        event_type,
                        event_detailed_type,
                        event_details,
                        **test_suite_result,
                    )
                    test_suite_event = self.test_suite_event_write(
                        test_suite_event_end,
                        test_suite_name,
                        self.config,
                        test_suite_config,
                        test_event_tag,
                        test_event_version,
                        timeplus_env,
                        timeplus_event_stream,
                    )
                    print(f"test_suite_event = {test_suite_event}")
                except BaseException as error:
                    logger.error(f"timeplus event write exception: {error}")
                    traceback.print_exc()
            self.test_suite_result_done_queue.put(test_suite_result_summary)
            if not test_suite_timeout_hit.is_set():
                self.test_suite_result_done_queue.join()
            timer.cancel()  # test_suite execution done, cancel timer otherwise the process will not terminate until timer done
            query_conn.send("tear_down")
            print(
                f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, waiting for message from query_execute......"
            )
            if not test_suite_timeout_hit.is_set():
                message_recv = query_conn.recv()
                print(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, mssage_recv from query_execute: {message_recv}"
                )
            else:
                print(
                    f"proton_setting = {proton_setting}, test_suite_name = {test_suite_name}, test_suite_timeout_hit.is_set(), bypass msssage_recv"
                )
            query_results_queue.close()
            query_conn.close()
            q_exec_client_conn.close()
            alive.value = False
            # q_exec_client.terminate()
            query_exe_client.join()
            del alive
            if client is not None:
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
                f"table drop {count} times, total time spent = {time_spent_drop}ms, avg_time_spent_drop = { avg_time_spent_drop}"
            )
        return test_run_list_len_total
