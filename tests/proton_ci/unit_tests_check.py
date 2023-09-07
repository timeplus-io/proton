#!/usr/bin/env python3

import logging
import os
import sys
import subprocess
# import atexit
import json,traceback
from typing import List, Tuple
#from timeplus import Stream, Environment
from proton_helper import ProtonHelper,prepare_event,prepare_event_tag
from unit_test_record import UnitTestRecord

from github import Github

from env_helper import CI, TEMP_PATH, REPO_COPY, REPORTS_PATH, GH_PERSONAL_ACCESS_TOKEN, PROTON_VERSION
from s3_helper import S3Helper
from pr_info import PRInfo
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from commit_status_helper import post_commit_status
# FIXME: refactor it to proton_helper
# from clickhouse_helper import (
#     ClickHouseHelper,
#     mark_flaky_tests,
#     prepare_tests_results_for_clickhouse,
# )
from stopwatch import Stopwatch
# from rerun_helper import RerunHelper
from tee_popen import TeePopen

if PROTON_VERSION is None:
    logging.error("PROTON_VERSION is None, could not find proton-unit-test image")
    sys.exit(1)

IMAGE_NAME = "ghcr.io/timeplus-io/proton-unit-test:" + PROTON_VERSION
TIMEOUT = 30 * 60

def get_test_name(line):
    elements = reversed(line.split(" "))
    for element in elements:
        if "(" not in element and ")" not in element:
            return element
    raise Exception(f"No test name in line '{line}'")


def process_results(
    result_folder: str,
) -> Tuple[str, str, List[Tuple[str, str]], List[str]]:
    OK_SIGN = "OK ]"
    FAILED_SIGN = "FAILED  ]"
    SEGFAULT = "Segmentation fault"
    SIGNAL = "received signal SIG"
    PASSED = "PASSED"

    summary = []  # type: List[Tuple[str, str]]
    total_counter = 0
    failed_counter = 0
    result_log_path = f"{result_folder}/test_result.txt"
    if not os.path.exists(result_log_path):
        logging.info("No output log on path %s", result_log_path)
        return "error", "No output log", summary, []

    status = "success"
    description = ""
    passed = False
    with open(result_log_path, "r", encoding="utf-8") as test_result:
        for line in test_result:
            if OK_SIGN in line:
                logging.info("Found ok line: '%s'", line)
                test_name = get_test_name(line.strip())
                logging.info("Test name: '%s'", test_name)
                summary.append((test_name, "OK"))
                total_counter += 1
            elif FAILED_SIGN in line and "listed below" not in line and "ms)" in line:
                logging.info("Found fail line: '%s'", line)
                test_name = get_test_name(line.strip())
                logging.info("Test name: '%s'", test_name)
                summary.append((test_name, "FAIL"))
                total_counter += 1
                failed_counter += 1
            elif SEGFAULT in line:
                logging.info("Found segfault line: '%s'", line)
                status = "failure"
                description += "Segmentation fault. "
                break
            elif SIGNAL in line:
                logging.info("Received signal line: '%s'", line)
                status = "failure"
                description += "Exit on signal. "
                break
            elif PASSED in line:
                logging.info("PASSED record found: '%s'", line)
                passed = True

    if not passed:
        status = "failure"
        description += "PASSED record not found. "

    if failed_counter != 0:
        status = "failure"

    if not description:
        description += (
            f"fail: {failed_counter}, passed: {total_counter - failed_counter}"
        )

    return status, description, summary, [result_log_path]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    reports_path = REPORTS_PATH

    test_type = "UnitTest" + PROTON_VERSION

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    pr_info = PRInfo()
    gh = Github(GH_PERSONAL_ACCESS_TOKEN, per_page=100)

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    test_output = os.path.join(temp_path, "test_output")
    if not os.path.exists(test_output):
        os.makedirs(test_output)

    run_command = f"docker run --cap-add=SYS_PTRACE --volume={test_output}:/test_output {docker_image}"

    run_log_path = os.path.join(test_output, "run.log")

    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path, timeout=TIMEOUT) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        elif retcode == -9:
            logging.error("Timeout {timeout} seconds exceeded".format(timeout=TIMEOUT))
            sys.exit(1)
        else:
            logging.info("Run failed")

    state, description, test_results, additional_logs = process_results(test_output)

    if CI:
        s3_helper = S3Helper("https://s3.amazonaws.com")
        report_url = upload_results(
            s3_helper,
            pr_info.number,
            pr_info.sha,
            test_results,
            [run_log_path] + additional_logs,
            test_type,
        )
        print(f"::notice ::Report url: {report_url}")

        post_commit_status(gh, pr_info.sha, test_type, description, state, report_url)

    #initialize test event fields
    repo_name = "proton"
    test_name = "unit_test"
    test_type = "ci_unit_test"
    test_id = None
    event_type = "unit_test_event"
    event_id = None
    timestamp = stopwatch.start_time_str
    timeplus_event_version = None

    #write status start test_event to timeplus 
    proton_helper = ProtonHelper()
    event = prepare_event(test_results,event_type)
    event_tag = prepare_event_tag(timeplus_event_version,repo_name,test_id,test_name,test_type,pr_info)
    record = UnitTestRecord(event_id,event,event_tag,timestamp)
    proton_helper.write(record)

    logging.info(state)
    logging.info(description)
    
    if state == "failure" or state == "error":
        sys.exit(1)
