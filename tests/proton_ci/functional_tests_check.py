#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from github import Github

from env_helper import CI, TEMP_PATH, REPO_COPY, REPORTS_PATH, GH_PERSONAL_ACCESS_TOKEN, PROTON_VERSION, PROTON_REPO, GITHUB_WORKSPACE
from s3_helper import S3Helper
from pr_info import PRInfo
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from proton_helper import ProtonHelper,prepare_event,prepare_event_tag
from functional_test_record import FunctionTestRecord
from stopwatch import Stopwatch
from tee_popen import TeePopen

if PROTON_VERSION is None:
    logging.error("PROTON_VERSION is None, could not find proton image")
    sys.exit(1)

if PROTON_REPO is None:
    logging.error("PROTON_REPO is None, could not find proton image")
    sys.exit(1)

IMAGE_NAME = PROTON_REPO + ":" + PROTON_VERSION
TIMEOUT = 90 * 60   # 90 minutes 
# aarch64 with asan running stateless need more time than 60 mins
# https://github.com/timeplus-io/proton/actions/runs/4726682507/jobs/8387999573#step:6:4081
DATASET_PATH = Path(REPO_COPY) / "docker/test/functional/datasets"


def get_build_image_command():
    return (
        f"docker build --build-arg FROM_TAG={PROTON_VERSION} --build-arg FROM_REPO={PROTON_REPO} -t timeplus/proton-functional-testrunner:{PROTON_VERSION} ."
    )

def get_run_command(check_name, output_path):
    env = []
    if check_name == "stateless":
        env.append("-e TEST_TYPE='--no-stateful --python'")
    elif check_name == "stateful":
        env.append("-e TEST_TYPE='--no-stateless'")
    else:
        logging.error(f"Not support check_name: {check_name}")
        sys.exit(1)
        
    env.append("-e MAX_CONCURRENT_QUERIES=200")
    env.append("-e MAX_CONCURRENT_INSERT_QUERIES=200")
    env.append("-e MAX_CONCURRENT_SELECT_QUERIES=200")

    # Add TSAN_OPTIONS if sanitize environment variable is set to 'thread'
    if os.getenv('sanitize') == 'thread':
        env.append("-e TSAN_OPTIONS='suppressions={GITHUB_WORKSPACE}/tests/proton_ci/sanitizer-thread-suppressions.txt'")
        logging.info("Using thread sanitizer suppressions.")

    env_str = " ".join(env)

    return (
        f"docker run --volume {output_path}:/test_output --volume {GITHUB_WORKSPACE}:/proton_src --cap-add=SYS_PTRACE {env_str} timeplus/proton-functional-testrunner:{PROTON_VERSION}"
    )

def process_results(
    result_folder: str,
    server_log_path: str,
) -> Tuple[str, str, List[Tuple[str, str]], List[str]]:
    test_results = []  # type: List[Tuple[str, str]]
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [
            f
            for f in os.listdir(result_folder)
            if os.path.isfile(os.path.join(result_folder, f))
        ]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    if os.path.exists(server_log_path):
        server_log_files = [
            f
            for f in os.listdir(server_log_path)
            if os.path.isfile(os.path.join(server_log_path, f))
        ]
        additional_files = additional_files + [
            os.path.join(server_log_path, f) for f in server_log_files
        ]

    status = []
    status_path = os.path.join(result_folder, "check_status.tsv")
    if os.path.exists(status_path):
        logging.info("Found test_results.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_folder))
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    results_path = os.path.join(result_folder, "test_results.tsv")

    if os.path.exists(results_path):
        logging.info("Found test_results.tsv")
    else:
        logging.info("Files in result folder %s", os.listdir(result_folder))
        return "error", "Not found test_results.tsv", test_results, additional_files

    with open(results_path, "r", encoding="utf-8") as results_file:
        test_results = list(csv.reader(results_file, delimiter="\t"))  # type: ignore
    if len(test_results) == 0:
        return "error", "Empty test_results.tsv", test_results, additional_files

    return state, description, test_results, additional_files

def prepare_build_image_dataset(path):
    subprocess.check_call("rm -rf {}/*".format(path), shell=True)

    if CI:
        s3_helper = S3Helper("https://s3.amazonaws.com")
        obj = s3_helper.download_test_dataset_from_s3("proton/dataset/tests/functional/", ".xz", path)
        logging.info(f"S3 Download dataset return: {obj}")
    else:
        local_path = Path(path)
        local_path.mkdir(parents=True, exist_ok=True)
        subprocess.check_call("curl https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz --output {}/visits_v1.tsv.xz".format(path), shell=True)
        subprocess.check_call("curl https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz --output {}/hits_v1.tsv.xz".format(path), shell=True)

    visits_path = Path(path) / "visits_v1.tsv.xz"
    hits_path = Path(path) / "hits_v1.tsv.xz"

    if visits_path.is_file() and hits_path.is_file():
        logging.info("Download datasets successfully")
    else:
        logging.info(f"Download datasets failed, file '{visits_path}' or '{hits_path}' doesn't exist")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    reports_path = REPORTS_PATH

    args = parse_args()
    check_name = args.check_name

    gh = Github(GH_PERSONAL_ACCESS_TOKEN, per_page=100)
    pr_info = PRInfo()

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    output_path = os.path.join(GITHUB_WORKSPACE, "output")
    server_log_path = os.path.join(output_path, "log")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    prepare_build_image_dataset(DATASET_PATH)

    build_log_path = os.path.join(output_path, "build.log")

    build_image_command = get_build_image_command()
    with TeePopen(build_image_command, build_log_path, cwd=GITHUB_WORKSPACE + "/docker/test/functional") as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Build image successfully")
        else:
            logging.error("Build image failed")
            sys.exit(1)

    run_log_path = os.path.join(output_path, "run.log")

    run_command = get_run_command(check_name, output_path)
    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path, timeout=TIMEOUT) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    state, description, test_results, additional_logs = process_results(output_path, server_log_path)

    if CI:
        s3_helper = S3Helper("https://s3.amazonaws.com")
        report_url = upload_results(
            s3_helper,
            pr_info.number,
            pr_info.sha,
            test_results,
            [run_log_path] + additional_logs,
            check_name,
        )
        print(f"::notice:: Report url: {report_url}")

    logging.info(state)
    logging.info(description)

    #initialize test event fields
    repo_name = "proton"
    test_name = "functional test"
    test_type = "ci_functional_test"
    test_id = None
    event_type = "functional_test_event"
    event_id = None
    timestamp = stopwatch.start_time_str
    timeplus_event_version = None

    #write status start test_event to timeplus 
    proton_helper = ProtonHelper()
    event = prepare_event(test_results,event_type,check_name=check_name)
    event_tag = prepare_event_tag(timeplus_event_version,repo_name,test_id,test_name,test_type,pr_info)
    record = FunctionTestRecord(event_id,event,event_tag,timestamp)
    proton_helper.write(record)

    if state != "success":
        sys.exit(1)
