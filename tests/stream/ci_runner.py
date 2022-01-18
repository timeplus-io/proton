import os, sys, logging, subprocess, time, datetime, json, csv, argparse
from helpers.s3_helper import S3Helper
from helpers.compress_files import compress_file_fast
import pytest

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


def ci_runner(local_all_results_folder_path, local_mode, pr_number="0", commit_sha="0"):

    timestamp = str(datetime.datetime.now())
    report_file_name = f"report_{timestamp}.html"
    report_file_path = f"{local_all_results_folder_path}/{report_file_name}"
    retcode = pytest.main(
        ["-s", "-v", "--log-cli-level=INFO", '--log-cli-format="%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)', '--log-cli-date-format="%Y-%m-%d %H:%M:%S"', f"--html={report_file_path}", "--self-contained-html"]
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

    if local_mode == False:
        pr_number = os.getenv("GITHUB_REF_NAME", pr_number)
        commit_sha = os.getenv("GITHUB_SHA", commit_sha)

        s3_helper = S3Helper("https://s3.amazonaws.com")

        report_url = upload_results(
            s3_helper,
            report_file_path,
            report_file_name,
            pr_number,
            commit_sha,
            *downloaded_log_files_paths,
        )
        print(f"::notice ::Report url: {report_url}")
    else:
        print("ci_runner: local mode, no report uploaded.")


if __name__ == "__main__":
    #logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    cur_dir = cur_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description="Run CI in local mode")
    parser.add_argument(
        "-l",
        "--local",
        action="store_true",
        help="run CI in default 'github' mode or 'local' mdoe",
    )
    args = parser.parse_args()
    if args.local:
        os.environ["PROTON_CI_MODE"] = "local"
    else:
        os.environ["PROTON_CI_MODE"] = "github"

    ci_runner(cur_dir, args.local)
