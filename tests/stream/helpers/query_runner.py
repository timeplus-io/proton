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
from helpers.rockets import tuple_2_list
from helpers.rockets import kill_query

from helpers.bucks import query_walk_through
from helpers.bucks import env_var_get

from helpers.bucks import reset_tables_of_test_inputs

from helpers.mz import *

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(processName)s %(module)s %(funcName)s %(message)s"
)
console_handler = logging.StreamHandler(sys.stderr)
console_handler.formatter = formatter
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


# todo: refactoring, Class Test and abstraction of test run logic in Rockets and reuse in performance test scripts.


def query_proton(statement_2_run, config, pyclient=None, query_log_csv=None):
    query = statement_2_run.get("query")
    query_id = str(statement_2_run.get("query_id"))
    query_sub_id = str(statement_2_run.get("query_sub_id"))
    query_type = statement_2_run.get("query_type")
    run_mode = statement_2_run.get("run_mode")
    result_keep = statement_2_run.get("result_keep")

    try:
        if pyclient == None:
            proton_server = config.get("proton_server")
            proton_server_native_port = config.get("proton_server_native_port")
            settings = {"max_block_size": 100000}
            pyclient = Client(
                host=proton_server, port=proton_server_native_port
            )  # create python client
            CLEAN_CLIENT = True
        if query_log_csv != None:
            writer = csv.writer(query_log_csv)
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
                    if query_log_csv != None:
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


def query_mz(statement_2_run, config, query_log_csv=None, query_kill_semaphore=None):
    query = statement_2_run.get("query")
    query_id = str(statement_2_run.get("query_id"))
    query_sub_id = str(statement_2_run.get("query_sub_id"))
    query_type = statement_2_run.get("query_type")
    run_mode = statement_2_run.get("run_mode")
    result_keep = statement_2_run.get("result_keep")
    if query_log_csv != None:
        writer = csv.writer(query_log_csv)
    mz = MaterializeDB()
    logger.debug(
        f"query_kill_semaphore = {query_kill_semaphore}, query_kill_semaphore.value = {query_kill_semaphore.value}"
    )
    mz.stream_query(
        query,
        query_log_csv=query_log_csv,
        query_kill_semaphore=query_kill_semaphore,
        show_data=True,
    )
    logger.debug(f"query_mz: running here...")


def query_ksql(file, statement_2_run, config):  # todo
    print()


def query_run_py(
    query_agent_id,
    statement_2_run,
    settings,
    query_done_semaphore,
    query_kill_semaphore,
    query_results_queue=None,
    config=None,
    pyclient=None,
):

    if config != None:
        rest_setting = config.get("rest_setting")
        table_ddl_url = rest_setting.get("table_ddl_url")

    config_set = statement_2_run.get(
        "config_set"
    )  # get the config set of the statement to learn which db engine and configs like host, port and .etc
    if config_set != None:
        query_config = config.get(config_set)
        db_engine = query_config.get("db_engine")

    query_record_file_name = (
        query_agent_id
        + "_"
        + "query_result_list"
        + str(datetime.datetime.now())
        + ".csv"
    )

    result_keep = statement_2_run.get("result_keep")

    if result_keep != None and result_keep == "True":
        with open(query_record_file_name, "w") as f:
            if db_engine == "proton":
                query_proton(
                    statement_2_run,
                    config,
                    query_log_csv=f,
                    query_kill_semaphore=query_kill_semaphore,
                )
            elif db_engine == "Materialize":
                query_mz(
                    statement_2_run,
                    config,
                    query_log_csv=f,
                    query_kill_semaphore=query_kill_semaphore,
                )
            elif db_engine == "KSQL":
                query_ksql(
                    f,
                    statement_2_run,
                    config,
                    query_log_csv=f,
                    query_kill_semaphore=query_kill_semaphore,
                )
    else:
        if db_engine == "proton":
            query_proton(
                statement_2_run, config, query_kill_semaphore=query_kill_semaphore
            )
        elif db_engine == "Materialize":
            query_mz(statement_2_run, config, query_kill_semaphore=query_kill_semaphore)
        elif db_engine == "KSQL":
            query_ksql(
                statement_2_run, config, query_kill_semaphore=query_kill_semaphore
            )

        # logger.debug(f"query_run_py: query_id = {query_id}, query = {query} to be execute.........")

    # logger.debug(f"query_run_py: query_id = {query_id}, query={query}, query_results = {query_results}")
    logger.info(f"query_run_py: ended.")
    return


def query_execute(config, child_conn, query_results_queue, alive):

    logger.debug(f"query_execute: query_execute starts...")

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
                logger.debug(
                    f"query_execute: test_steps_done received @ {datetime.datetime.now()}, query_procs={query_procs}"
                )
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
                        query_kill_semaphore = proc.get("query_kill_semaphore")
                        db_engine = proc.get("db_engine")
                        logger.debug(
                            f"query_execute: query_procs pop, process = {process}, exitcode = {exitcode}"
                        )

                        if process.exitcode != None:
                            i += 1
                        # else:
                        elif terminate == "auto":
                            if db_engine != None and db_engine == "Materialize":
                                query_kill_semaphore.value = True  # set query_kill_semaphore to True to notify MZ object to quite from while True
                                logger.debug(
                                    f"db_engine = {db_engine}, query_kill_semaphore = {query_kill_semaphore}"
                                )
                            else:
                                kill_query(client, query_sub_id)
                                logger.debug(
                                    f"query_execute: kill_query query_id = {query_sub_id} is executed."
                                )

                        logger.debug(
                            f"query_execute: query_procs = {query_procs} after trying to remove"
                        )
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
                db_engine = None
                config_set = statement_2_run.get("config_set")
                if config_set != None:
                    query_config = config.get(config_set)
                    db_engine = query_config.get("db_engine")
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
                    query_kill_semaphore = mp.Value("b", False)
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
                        query_kill_semaphore,
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
                            "query_kill": query_kill_semaphore,
                            "query_done": query_done_semaphore,
                            "db_engine": db_engine,
                        }
                    )

                    logger.debug(
                        f"query_execute: start a proc for query = {query}, query_proc.pid = {query_proc.pid}"
                    )

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
            query_kill_semaphore = proc.get("query_kill")
            query_done_semaphore = proc.get("query_done")
            process.terminate()
            process.join()
            del query_done_semaphore
            del query_done_semaphore
    client.disconnect()
    # query_exe_queue.put("tear_down_done")
    child_conn.send("tear_down_done")
    logger.debug(f"query_execute: tear_down completed and end")


def test_suite_run(test_context, proc_target_func=query_execute, case_2_run="all"):
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

    if case_2_run != "all":
        for test in tests:
            test_id = test.get("id")
            if case_2_run == str(test_id):
                test_run_list.append(test)
    else:
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
            # logging.debug(f"rockets_run: test_id = {test_id}, test_case = {test_case}, steps = {steps}")
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

                    logging.info(
                        f"rockets_run: {test_id_run}, test_id = {test_id}, step{step_id}.statements{statements_id}, done..."
                    )

                    statements_id += 1

                step_id += 1

            input_msg = None

            FIFO = "query_runner_pipe"

            if os.path.exists(FIFO):
                os.unlink(FIFO)
                os.mkfifo(FIFO)
            else:
                os.mkfifo(FIFO)
            print("Waiting for any msg to query_runner_pipe as quit signal...")
            with open(FIFO) as fifo:
                while True:
                    data = fifo.read()
                    if len(data) == 0:
                        print("quit msg recevived in query_runner_pipe, quit...")
                        fifo.close()
                        break

            query_conn.send("test_steps_done")

            message_recv = (
                query_conn.recv()
            )  # wait the query_execute to send "case_result_done" to indicate all the statements in pipe are consumed.

            logger.debug(
                f"test_suite_run: mssage_recv from query_execute = {message_recv}"
            )

            assert message_recv == "case_result_done"

            for (
                input_proc
            ) in input_procs:  # tear down all input processes to write input_record
                input_tear_down = input_proc.get("input_tear_down")
                input_tear_down.value = True

            test_id_run += 1

        # test_sets = result_collect(): Todo
    except (BaseException) as error:
        logging.info("exception:", error)
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
            proc["proc"].join()
        logger.debug(f"test_suite_run: query processes tear down ......")
        if os.path.exists(FIFO):
            os.unlink(FIFO)  # delete pipe file
        query_exe_client.join()
        del alive

        return test_sets


def test_suite_context(config_file=None, tests_file=None, docker_compose_file=None):
    # global alive
    # logging.debug(f"test_context: proc_target_func = {proc_target_func}, config_file = {config_file}, tests_file = {tests_file}, docker_compose_file = {docker_compose_file}")
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
    logging_config_file = f"{cur_file_path}/logging.conf"
    case_2_run = "all"
    if os.path.exists(logging_config_file):
        logging.basicConfig(
            format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
        )  # todo: add log stuff
        logging.config.fileConfig(logging_config_file)  # need logging.conf
        logger = logging.getLogger("rockets")
    else:
        logging.info("no logging.conf exists under ../helper, no logging.")

    argv = sys.argv[1:]  # get -d to specify the test_sutie path
    try:
        opts, args = getopt.getopt(argv, "d:i:q")
    except (BaseException) as error:
        logging.info(error)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-d"]:
            test_suite_path = arg
        elif opt in ["-i"]:
            case_2_run = arg
        elif opt in ["-q"]:
            FIFO = "query_runner_pipe"
            if os.path.exists(FIFO) == True:
                print("Send quit msg to query_runner_pipe")
                with open(FIFO, "w") as fifo:
                    fifo.writelines("quit")
                    fifo.close()
            sys.exit(0)

    logging.info("query_runner starts......")
    print(f"test_suite_path = {test_suite_path}")
    print(f"case_2_run = {case_2_run}")

    if test_suite_path == None:
        logging.info("No test suite directory specificed by -d, exit.")
        sys.exit(0)
    config_file = f"{test_suite_path}/configs/config.json"
    tests_file = f"{test_suite_path}/tests.json"
    docker_compose_file = f"{test_suite_path}/configs/docker-compose.yaml"

    if os.path.exists(tests_file):
        test_context = test_suite_context(
            config_file, tests_file, docker_compose_file
        )  # need to have config env vars/config.json and test.json when run rockets.py as a test debug tooling.
        test_sets = test_suite_run(test_context, query_execute, case_2_run)
        # output the test_sets one by one
        logging.info("main: ouput test_sets......")
    else:
        logging.info("No tests.json exists under test suite folder.")
