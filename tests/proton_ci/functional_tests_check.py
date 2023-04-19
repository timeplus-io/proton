#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import sys
from typing import List, Tuple

from github import Github

from env_helper import CI, TEMP_PATH, REPO_COPY, REPORTS_PATH, GH_PERSONAL_ACCESS_TOKEN, PROTON_VERSION, GITHUB_WORKSPACE
from s3_helper import S3Helper
from pr_info import PRInfo
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from stopwatch import Stopwatch
from tee_popen import TeePopen

if PROTON_VERSION is None:
    logging.error("PROTON_VERSION is None, could not find proton image")
    sys.exit(1)

IMAGE_NAME = "timeplus/proton:" + PROTON_VERSION
TIMEOUT = 90 * 60   # 90 minutes 
# aarch64 with asan running stateless need more time than 60 mins
# https://github.com/timeplus-io/proton/actions/runs/4726682507/jobs/8387999573#step:6:4081


def get_build_image_command():
    return (
        f"docker build --build-arg FROM_TAG={PROTON_VERSION} -t timeplus/proton-functional-testrunner:{PROTON_VERSION} ."
    )

def get_run_command(check_name, output_path):
    env = []
    if check_name == "stateless":
        env.append("-e TEST_TYPE='--no-stateful'")
    elif check_name == "stateful":
        env.append("-e TEST_TYPE='--no-stateless'")
    else:
        logging.error(f"Not support check_name: {check_name}")
        sys.exit(1)

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

    # TODO: ProtonHelper, upload test result to timeplus cloud

    if state != "success":
        sys.exit(1)

