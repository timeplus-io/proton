import os, sys, logging, subprocess, time, datetime, json, argparse, traceback
from argparse import ArgumentParser
from helpers.s3_helper import S3Helper
from helpers.utils import enhanced_compose_up
from helpers.event_util import Event, EventRecord, TestEventTag
import multiprocessing as mp
import pytest
from timeplus import Stream, Environment


logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

PROTON_PYTHON_DRIVER_S3_BUCKET_NAME = "tp-internal"
PROTON_PYTHON_DRIVER_NANME = "proton-driver"

cur_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = f"{cur_dir}/test_stream_smoke/configs/config.json"

docker_compose_file_path = f"{cur_dir}/test_stream_smoke/configs/docker-compose.yaml"
DEFAULT_TEST_SUITE_TIMEOUT = 900  # seconds
DEFAULT_TEST_FOLDER = "test_stream_smoke"


def compress_logs(self, dir, relpaths, result_path):
    subprocess.check_call(
        "tar czf {} -C {} {}".format(result_path, dir, " ".join(relpaths)), shell=True
    )  # STYLE_CHECK_ALLOW_SUBPROCESS_CHECK_CALL


def container_file_download(dir="./", setting="nativelog", *files_in_container):
    print(files_in_container)
    if len(files_in_container) != 0:
        files_downloaded = []
        for (
            file_tuple
        ) in (
            files_in_container
        ):  # file is a tuple of {proton_server_container_name,log_file_path}
            try:
                print(f"file_tuple = {file_tuple}")
                file_name = file_tuple[1].split("/")[-1]
                file_name = f"{setting}-{file_tuple[0]}-" + file_name
                print(f"file_name = {file_name}")
                cmd = f"docker cp {file_tuple[1]} {dir}{file_name}"
                print(f"Copying {file_tuple[1]}, command = {cmd}")
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
                files_downloaded.append(f"{dir}{file_name}")
            except Exception as ex:
                # time.sleep(i * 3)
                print(f"Got execption copying file {ex} ")
        return tuple(files_downloaded)
    else:
        return ()


def upload_results(
    s3_client,
    report_file_path,
    s3_report_name=None,
    pr_number="0",
    commit_sha="",
    *additional_files_paths,
):
    if s3_report_name == None:
        s3_report_name = "report.html"
    s3_path_prefix = f"reports/proton/tests/CI/{pr_number}/{commit_sha}/"
    report_urls = []  # list of report file and log files
    report_url = s3_client.upload_test_report_to_s3(
        report_file_path, s3_path_prefix + s3_report_name
    )
    report_urls.append(report_url)
    if len(additional_files_paths) > 0:
        for additional_file_path in additional_files_paths:
            logging.debug(
                f"upload_results: additional_file_path = {additional_file_path}"
            )
            file_name = additional_file_path.rsplit("/", 1)[1]
            url = s3_client.upload_test_report_to_s3(
                additional_file_path, s3_path_prefix + file_name
            )
            report_urls.append(url)
    logging.info("Search result in url %s", report_urls)
    return report_urls


def upload_proton_logs(s3_client, proton_log_folder, pr_number="0", commit_sha=""):
    s3_proton_log_folder = (
        f"reports/proton/tests/CI/{pr_number}/{commit_sha}/proton_logs"
    )
    proton_log_url = s3_client.upload_test_folder_to_s3(
        proton_log_folder, s3_proton_log_folder
    )
    logging.info("Search result in url %s", proton_log_url)
    return proton_log_url


def ci_runner(
    local_all_results_folder_path,
    setting_config,
    test_result_shared_list,
    test_folders_list,
    run_mode="local",
    pr_number="0",
    commit_sha="0",
    setting="default",
    logging_level="INFO",
):
    timestamp = str(datetime.datetime.now())
    setting_running_start = datetime.datetime.now()
    report_file_name = f"report_{setting}_{timestamp}.html"
    report_file_path = f"{local_all_results_folder_path}/{report_file_name}"
    proton_log_folder = f"{local_all_results_folder_path}/proton"
    pytest_logging_level_set = f"--log-cli-level={logging_level}"
    s3_helper = S3Helper("https://s3.amazonaws.com")
    multi_protons = setting_config.get("multi_protons")
    global_sql_settings = setting_config.get("sql_settings")
    global_sql_settings_str = json.dumps(
        global_sql_settings
    )  # todo: unify the procedure of ci_runner env var setting and passing to rockets
    os.environ["SQL_SETTINGS"] = global_sql_settings_str
    proton_server_container_name_list = []
    test_result = ""
    if (
        multi_protons == True
    ):  # if multi_protons is True, there are multiple settings for allocating the test suites on configs
        for key in setting_config["settings"]:
            proton_server_container_name_str = setting_config["settings"][key].get(
                "proton_server_container_name"
            )
            ci_runner_params_from_config = setting_config["settings"][key].get(
                "ci_runner_params"
            )  # todo: support ci_runner_params per env of multi envs setting
            if proton_server_container_name_str is None:
                raise Exception(
                    f"proton_server_container_name of setting = {setting} is not found in setting_config"
                )
            else:
                proton_server_container_name_list.extend(
                    proton_server_container_name_str.split(",")
                )
    else:  # todo: right now just make a simple if_else to handle the logic, so currently only multi env settings for single node proton is supported, need to optimize to support multi envs of cluster
        proton_server_container_name_str = setting_config.get(
            "proton_server_container_name"
        )
        if proton_server_container_name_str is None:
            raise Exception(
                f"proton_server_container_name of setting = {setting} is not found in setting_config"
            )
        proton_server_container_name_list = proton_server_container_name_str.split(
            ","
        )  # for multi containers in clustering settings
        ci_runner_params_from_config = setting_config.get("ci_runner_params")
        if (
            ci_runner_params_from_config is not None
            and len(ci_runner_params_from_config) > 0
        ):  # todo: currently only single env setting support ci_runner_params, need to optimize to support ci_runner_params per env
            for param in ci_runner_params_from_config:
                print(f"ci_runner: setting = {setting}, param = {param}")
                for key, value in param.items():
                    os.environ[key] = value
                    env_setting = os.getenv(key)
                    print(f"os.getenv({key}) = {env_setting}")
    os.environ[
        "PROTON_SETTING"
    ] = setting  # set the env virable to setting for rockets_run() based on settings gotten from cmdline
    proton_logs_in_container = []  # a list of tuple
    print(f"proton_server_container_name_list = {proton_server_container_name_list}")
    for proton_server_container_name in proton_server_container_name_list:
        proton_log_in_container = (
            f"{proton_server_container_name}://var/log/proton-server/proton-server.log"
        )
        proton_logs_in_container.append(
            (proton_server_container_name, proton_log_in_container)
        )
        proton_err_log_in_container = f"{proton_server_container_name}://var/log/proton-server/proton-server.err.log"
        proton_logs_in_container.append(
            (proton_server_container_name, proton_err_log_in_container)
        )
    pytest_args = [
        "-s",
        "-v",
        pytest_logging_level_set,
        "--log-cli-format=%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)",
        "--log-cli-date-format=%Y-%m-%d %H:%M:%S",
        f"--html={report_file_path}",
        "--self-contained-html",
    ]
    for test_folder in test_folders_list:
        pytest_args.append(test_folder)
    retcode = pytest.main(pytest_args)
    if test_result_shared_list != None:
        test_result_shared_list.append({"proton_setting": setting, "retcode": retcode})
    setting_running_test_end = datetime.datetime.now()
    setting_test_duration = setting_running_test_end - setting_running_start
    print(
        f"setting = {setting}, setting_test_duration = {setting_test_duration.seconds}"
    )

    with open(".status", "a+") as status_result:
        status_result.writelines(f"{setting}:" + str(retcode))
    downloaded_log_files_paths = []
    downloaded_log_files_paths = container_file_download(
        "./", setting, *proton_logs_in_container
    )
    print(f"ci_runner: downloaded_log_files_paths = {downloaded_log_files_paths}")
    if run_mode == "github":
        pr_number = os.getenv("GITHUB_REF_NAME", pr_number)
        commit_sha = os.getenv("GITHUB_SHA", commit_sha)
        if retcode == 0:
            report_urls = upload_results(
                s3_helper, report_file_path, report_file_name, pr_number, commit_sha
            )
        else:
            report_urls = upload_results(
                s3_helper,
                report_file_path,
                report_file_name,
                pr_number,
                commit_sha,
                *downloaded_log_files_paths,
            )
        for report_url in report_urls:
            report_url = report_url.replace("https://s3.amazonaws.com/", "s3://")
            report_url = report_url.replace("%20", " ")
            print(f"::notice ::Report/Log s3 uri: {report_url}")
            print(
                f"::notice ::Report/Log download command: aws s3 cp '{report_url}' ./"
            )
            proton_log_folder_url = upload_proton_logs(
                s3_helper,
                proton_log_folder,
                pr_number,
                commit_sha,
            )
            print(f"::notice ::Proton server log url: {proton_log_folder_url}")
    else:
        print("ci_runner: local mode, no report uploaded.")
    return retcode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CI in local mode")
    run_mode = "github"
    loop = 1
    logging_level = "ERROR"
    os.environ["PROTON_TEST_IDS"] = "all"
    test_suites = "all"
    settings = []
    ci_runner_start = datetime.datetime.now()
    ci_runner_end = datetime.datetime.now()
    ci_runner_duration = ci_runner_end - ci_runner_start
    test_result_shared_list = (
        []
    )  # shared list for collecting result from ci_runner processes
    mp_mgr = mp.Manager()
    test_result_shared_list = mp_mgr.list()
    configs = None
    parser = ArgumentParser(description="Proton functional tests")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--github",
        action="store_const",
        const="github",
        default="github",
        dest="run_mode",
        help="Run tests in github action flow mode",
    )
    group.add_argument(
        "--local",
        action="store_const",
        const="local",
        default=None,
        dest="run_mode",
        help="Run tests in local mode",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Run tests in debug mode to print debug log, otherwiese info log ouput",
    )
    parser.add_argument(
        "-d",
        "--test_folders",
        default=DEFAULT_TEST_FOLDER,
        help="test folders ci_runner will run tests in",
    )
    parser.add_argument(
        "-q", "--test_suites", default=None, help="Test suite name to be run"
    )
    parser.add_argument("-i", "--id", help="Test suite name to be run")
    parser.add_argument(
        "--loop",
        default=1,
        type=int,
        help="Num of repeating running tests",
    )
    parser.add_argument(
        "-s",
        "--settings",
        default="default",
        help="settings of proton for testing running on, mapping to the proton settings in proton/tests/stream/test_stream_smoke/configs/config.json",
    )
    parser.add_argument(
        "--cluster_query_route_mode",
        help="how to run query on cluster, e.g. none_stream_node_first",
    )
    parser.add_argument(
        "--cluster_query_node",
        help="cluster node name for running queries on, e.g. proton-cluster-node1",
    )
    parser.add_argument(
        "--create_stream_shards",
        help="shards to be created when creating stream, if set settings shards = <create_stream_shards> will be added to all the stream creating statement",
    )
    parser.add_argument(
        "--create_stream_replicas",
        help="replicas to be created when creating stream, if set settings replicas = <create_stream_replicas> will be added to all the stream creating statement",
    )
    parser.add_argument(
        "--no_retry", action="store_true", default=False, help="Run tests without retry"
    )
    parser.add_argument(
        "--test_suite_timeout",
        default=DEFAULT_TEST_SUITE_TIMEOUT,
        type=int,
        help="Test suite running time out timer, in seconds, 1200 seconds by default",
    )
    parser.add_argument("--tmp", help="Path to tmp dir")
    args = parser.parse_args()
    envs = []
    if args.run_mode:
        run_mode = args.run_mode
        os.environ["PROTON_CI_MODE"] = run_mode
    if args.debug:
        logging_level = "DEBUG"
    settings = args.settings.split(",")
    test_folders = args.test_folders
    test_folders_list = args.test_folders.split(",")
    if args.test_suites:
        os.environ["PROTON_TEST_SUITES"] = args.test_suites
    if args.loop:
        loop = args.loop
    if args.id:
        os.environ["PROTON_TEST_IDS"] = args.id
    if args.cluster_query_route_mode:
        os.environ["PROTON_CLUSTER_QUERY_ROUTE_MODE"] = args.cluster_query_route_mode
    if args.cluster_query_node:
        os.environ["PROTON_CLUSTER_QUERY_NODE"] = args.cluster_query_node
    if args.create_stream_shards:
        os.environ["PROTON_CREATE_STREAM_SHARDS"] = args.create_stream_shards
    if args.create_stream_replicas:
        os.environ["PROTON_CREATE_STREAM_REPLICAS"] = args.create_stream_replicas
    if args.no_retry is True:
        test_retry = "False"
    else:
        test_retry = "True"
    os.environ["TEST_RETRY"] = test_retry
    # if args.test_suite_timeout:
    test_suite_timeout = args.test_suite_timeout
    os.environ["TEST_SUITE_TIMEOUT"] = str(test_suite_timeout)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    if logging_level == "INFO":
        logger.setLevel(logging.INFO)
    elif logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "WARNING":
        logger.setLevel(logging.WARNING)
    elif logging_level == "ERROR":
        logger.setLevel(logging.ERROR)
    elif logging_level == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.INFO)
    print(
        f"ci_runner starting: run_mode = {run_mode}, loop = {loop}, logging_level={logging_level}, test_retry = {test_retry}, test_suite_timeout = {test_suite_timeout}, test_folders = {test_folders} starts"
    )
    if run_mode == "github":
        pr_number = os.getenv("GITHUB_REF_NAME", "0")
        commit_sha = os.getenv("GITHUB_SHA", "0")
    else:
        pr_number = "0"
        commit_sha = "0"
    args_vars = vars(args)
    args_dict = {}
    for key, value in args_vars.items():
        if value is None:
            value = "None"
        arg_dict = {key: value}
        args_dict = {**args_dict, **arg_dict}
    print(f"args_dict = {args_dict}")
    with open(config_file_path) as f:
        configs = json.load(f)
    timeplus_event_stream = configs.get(
        "timeplus_event_stream"
    )  # todo: distribute global configs into configs
    timeplus_event_version = configs.get("timeplus_event_version")
    # initialize test event fields
    test_command_details = {
        "test_command_details": {
            "test_program": __file__.split("/")[-1],
            "test_paras": args_dict,
        }
    }  # put ci_runner parameters into tag of test_event
    event_id = None
    repo_name = "proton"
    test_name = "ci_runner"
    test_type = "ci_smoke"
    event_type = "test_event"
    event_detailed_type = "status"
    # stream_name = 'test_event_2' #todo: read from test config
    api_key = os.environ.get("TIMEPLUS_API_KEY2", None)
    api_address = os.environ.get("TIMEPLUS_ADDRESS2", "")
    work_space = os.environ.get("TIMEPLUS_WORKSPACE2", None)
    if work_space is not None and work_space != "":
        api_address = api_address + "/" + work_space
    sanitizer = os.environ.get("SANITIZER", "")
    if len(sanitizer) == 0:
        build_type = "release_build"
    else:
        build_type = sanitizer
    os_info = os.getenv("RUNNER_OS", "Linux")
    pr_number = os.getenv("GITHUB_REF_NAME", "0")
    commit_sha = os.getenv("GITHUB_SHA", "0")
    platform_info = os.getenv("RUNNER_ARCH", "x86_64")
    # version = "0.1"
    test_id = None
    event_id = None
    test_result = ""
    test_event_tag = None
    timeplus_env = None
    if api_address is not None and api_key is not None:
        try:  # write status start test_event to timeplus
            timeplus_env = Environment().address(api_address).apikey(api_key)
            test_event_tag = TestEventTag.create(
                repo_name,
                test_id,
                test_name,
                test_type,
                build_type,
                pr_number,
                commit_sha,
                os_info,
                platform_info,
            )
            event_details = "start"
            test_event_start = Event.create(
                event_type, event_detailed_type, event_details
            )
            test_event_record_start = EventRecord.create(
                None, test_event_start, test_event_tag, timeplus_event_version
            )
            if test_event_tag:
                test_info_tag = test_event_tag.test_info_tag
                os.environ["TIMEPLUS_TEST_EVENT_TAG"] = json.dumps(
                    test_event_tag.value
                )  # set env var for test_id to pass to rockets
            if test_event_start:
                test_event_record_start.write(timeplus_env, timeplus_event_stream)
            print(f"test_event_start sent")
            # test_event_tag_from_env = os.getenv("TIMEPLUS_TEST_EVENT_TAG")
            # print(f"test_event_tag_from_env = {test_event_tag_from_env}")
        except BaseException as error:
            logger.error(f"timeplus event write exception: {error}")
            traceback.print_exc()
    else:
        print(
            f"one of TIMEPLUS_API_KEY2,TIMEPLUS_ADDRESS2,TIMEPLUS_WORKSPACE2 is not found in ENV"
        )

    if (
        "test_production_compatibility" in test_folders_list
    ):  # todo: hardcode now, refactor later to have a system_test_runner to run all pytests folders like test_production_compatibility
        settings = ["default"]
        docker_compose_file_path = (
            f"{cur_dir}/test_production_compatibility/configs/docker-compose.yaml"
        )
    if run_mode == "local":
        env_docker_compose_res = True
        logger.info(f"Bypass docker-compose up.")
    else:
        env_docker_compose_res = enhanced_compose_up(docker_compose_file_path)
        logger.info(f"docker-compose up...")
    logger.debug(f"env_docker_compose_res: {env_docker_compose_res}")
    if not env_docker_compose_res:
        raise Exception(f"Env docker-compose up failure. path = {docker_compose_file_path}")
    if settings == []:
        # settings = ["nativelog"]
        settings = ["default"]
    procs = []
    for setting in settings:
        logger.debug(f"setting = {setting}, get config...")
        # with open(config_file_path) as f:
        #     configs = json.load(f)
        setting_config = configs.get(
            setting
        )  # if settings is not null, then read different setting config and start processes
        if setting_config is None:
            raise Exception(
                f"no config for setting = {setting} found in {config_file_path}"
            )
        logger.debug(
            f"ci_runner: setting_config for setting = {setting} = {setting_config}"
        )
        args = (
            cur_dir,
            setting_config,
            test_result_shared_list,
            test_folders_list,
            run_mode,
            "0",
            "0",
            setting,
            logging_level,
        )
        proc = mp.Process(target=ci_runner, args=args)
        proc.start()
        # logger.debug(f"args = {args}, ci_runner proc starts...")
        procs.append(proc)
        time.sleep(5)
    for proc in procs:
        proc.join()
    ci_runner_end = datetime.datetime.now()
    ci_runner_duration = ci_runner_end - ci_runner_start
    logger.info(
        f"ci_runner end: run_mode = {run_mode}, loop = {loop}, logging_level={logging_level}, test_suite_timeout = {test_suite_timeout}, ci_runner_duration = {ci_runner_duration.seconds} seconds, ends"
    )
    test_result_flag = 1
    detailed_summary = []
    for (
        item
    ) in (
        test_result_shared_list
    ):  # retcode: {'proton_setting': 'default', 'retcode': <ExitCode.TESTS_FAILED: 1>}:
        setting = item.get("proton_setting")
        retcode_str = str(item.get("retcode"))
        if "OK" not in retcode_str:
            test_result_flag = test_result_flag * 0
        detailed_summary.append({setting: retcode_str})
    if test_result_flag:
        test_result_str = "success"
    else:
        test_result_str = "failed"
    logger.info(f"test_result_str = {test_result_str}")
    test_result = {"test_result": test_result_str, "detailed_summary": detailed_summary}
    if api_address is not None and api_key is not None:
        try:  # write status start test_event to timeplus
            event_details = "end"
            test_event_end = Event.create(
                event_type, event_detailed_type, event_details, **test_result
            )
            test_event_record_end = EventRecord.create(
                None, test_event_end, test_event_tag, timeplus_event_version
            )
            print(f"test_event_end = {test_event_end}")
            if test_event_record_end:
                test_event_record_end.write(timeplus_env, timeplus_event_stream)
        except BaseException as error:
            logger.error(f"timeplus event write exception: {error}")
            traceback.print_exc()
