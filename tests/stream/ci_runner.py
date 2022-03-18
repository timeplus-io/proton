import os, sys, logging, subprocess, time, datetime, json, csv, argparse, getopt
from helpers.s3_helper import S3Helper
from helpers.compress_files import compress_file_fast
import pytest

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)") 

PROTON_PYTHON_DRIVER_S3_BUCKET_NAME = "tp-internal"
PROTON_PYTHON_DIRVER_S3_OBJ_NAME = "proton/proton-python-driver/clickhouse-driver-0.2.4.tar.gz"
PROTON_PYTHON_DRIVER_FILE_NAME ="clickhouse-driver-0.2.4.tar.gz"

proton_log_in_container = "proton-server:/var/log/proton-server/proton-server.log"
proton_err_log_in_container = (
    "proton-server:/var/log/proton-server/proton-server.err.log"
)


def compress_logs(self, dir, relpaths, result_path):
    subprocess.check_call(
        "tar czf {} -C {} {}".format(result_path, dir, " ".join(relpaths)), shell=True
    )  # STYLE_CHECK_ALLOW_SUBPROCESS_CHECK_CALL


def container_file_download(dir="./", *files_in_container):
    if len(files_in_container) != 0:
        files_downloaded = []
        for file in files_in_container:
            try:
                file_name = file.rsplit("/", 1)[1]
                cmd = f"docker cp {file} {dir}/"
                logging.info(f"Copying {file}, command = {cmd}")
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
                files_downloaded.append(f"{dir}/{file_name}")
            except Exception as ex:
                # time.sleep(i * 3)
                logging.info(f"Got execption copying file {ex} ")
        return tuple(files_downloaded)
    else:
        return ()


def upload_results(
    s3_client,
    report_file_path,
    s3_report_name=None,
    pr_number=0,
    commit_sha="",
    *additional_files_paths,
):
    if s3_report_name == None:
        s3_report_name = "report.html"
    s3_path_prefix = f"reports/proton/tests/CI/{pr_number}/{commit_sha}/"
    report_url = s3_client.upload_test_report_to_s3(
        report_file_path, s3_path_prefix + s3_report_name
    )
    if len(additional_files_paths) > 0:
        for additional_file_path in additional_files_paths:
            logging.debug(
                f"upload_results: additional_file_path = {additional_file_path}"
            )
            file_name = additional_file_path.rsplit("/", 1)[1]
            s3_client.upload_test_report_to_s3(
                additional_file_path, s3_path_prefix + file_name
            )

    logging.info("Search result in url %s", report_url)
    return report_url


def upload_proton_logs(
    s3_client,
    proton_log_folder,
    pr_number=0,
    commit_sha=""
):
    s3_proton_log_folder = f"reports/proton/tests/CI/{pr_number}/{commit_sha}/proton_logs"
    proton_log_url = s3_client.upload_test_folder_to_s3(
        proton_log_folder, s3_proton_log_folder
    )
    logging.info("Search result in url %s", proton_log_url)
    return proton_log_url


def ci_runner(local_all_results_folder_path, run_mode = 'local', pr_number="0", commit_sha="0", logging_level = "INFO"):
    timestamp = str(datetime.datetime.now())
    report_file_name = f"report_{timestamp}.html"
    report_file_path = f"{local_all_results_folder_path}/{report_file_name}"
    proton_log_folder = f"{local_all_results_folder_path}/proton"
    pytest_logging_level_set = f"--log-cli-level={logging_level}"

    s3_helper = S3Helper("https://s3.amazonaws.com")
    s3_helper.client.download_file(PROTON_PYTHON_DRIVER_S3_BUCKET_NAME, PROTON_PYTHON_DIRVER_S3_OBJ_NAME, PROTON_PYTHON_DRIVER_FILE_NAME)
    logger.debug(f"{PROTON_PYTHON_DRIVER_FILE_NAME} is downloaded")
    command = "pip3 install ./" + PROTON_PYTHON_DRIVER_FILE_NAME
    ret = ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=600)
    logger.debug(f"ret of subprocess.run({command}) = {ret}")

    retcode = pytest.main(
        ["-s", "-v", pytest_logging_level_set, '--log-cli-format=%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)', '--log-cli-date-format=%Y-%m-%d %H:%M:%S', f"--html={report_file_path}", "--self-contained-html"]
    )

    with open(".status", "w") as status_result:
        status_result.write(str(retcode))

    downloaded_log_files_paths = container_file_download(
        local_all_results_folder_path,
        proton_log_in_container,
        proton_err_log_in_container,
    )



    logging.debug(
        f"ci_runner: downloaded_log_files_paths = {downloaded_log_files_paths}"
    )

    if run_mode == 'github':
        pr_number = os.getenv("GITHUB_REF_NAME", pr_number)
        commit_sha = os.getenv("GITHUB_SHA", commit_sha)

        #s3_helper = S3Helper("https://s3.amazonaws.com")

        report_url = upload_results(
            s3_helper,
            report_file_path,
            report_file_name,
            pr_number,
            commit_sha,
            *downloaded_log_files_paths,
        )
        print(f"::notice ::Report url: {report_url}")

        proton_log_folder_url = upload_proton_logs(
            s3_helper,
            proton_log_folder,
            pr_number,
            commit_sha,
        )
        print(f"::notice ::Proton server log url: {proton_log_folder_url}")
    else:
        print("ci_runner: local mode, no report uploaded.")


if __name__ == "__main__":
    #logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    cur_dir = cur_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description="Run CI in local mode")
    run_mode = 'github'
    loop = 1
    logging_level = "INFO"
    os.environ["PROTON_TEST_IDS"] = "all"
    test_suites = "all"

    try:
        opts, args = getopt.getopt(sys.argv[1:], '', ["local", "debug", "test_suites=", "loop=", "id="])
    except(getopt.GetoptError) as error:
        print(f"command error: {error}")
        print(f"usage: python3 ci_runner.py --local --debug --test_suites=smoke,materilize --loop=30 --id=1,2,3")
        sys.exit(1)
    print(f"opts = {opts}")
    for name, value in opts:
        if name in ("--local", "-l"):
            os.environ["PROTON_CI_MODE"] = "local"
            run_mode = 'local'
        
        if name in ("--debug"):
            logging_level = "DEBUG"
        
        if name in ("--test_suites"):
            os.environ["PROTON_TEST_SUITES"] = value

        if name in ("--loop"):
            if value.isdigit() == False:
                print(f"usage: python3 ci_runner.py --local --loop=30")
                sys.exit(1)
            else:
                loop = int(value)
        if name in ("--id"):
            os.environ["PROTON_TEST_IDS"] = value 
    print(f"ci_runner: run_mode = {run_mode}, loop = {loop}, logging_level={logging_level}")
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    if logging_level == "INFO":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    i = 0
    while i < loop:
        ci_runner(cur_dir, run_mode, logging_level = logging_level)
        i += 1






