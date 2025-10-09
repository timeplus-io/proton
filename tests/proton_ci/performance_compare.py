import json
import os
import sys
import argparse
import logging
import pandas as pd

from env_helper import PROTON_VERSION, SANITIZER
from version_helper import VersionHelper

RETRY = 5

if PROTON_VERSION is None:
    logging.error("PROTON_VERSION is None, could not find proton image")
    sys.exit(1)


## Load and format data
def load_json(source_dir):
    if not os.path.exists(source_dir):
        os.makedirs(source_dir)

    merged_list=[]
    for filename in os.listdir(source_dir):
        source_file = os.path.join(source_dir, filename)
        if not os.path.isfile(source_file):
            continue
        if not filename.endswith('.json'):
            continue

        with open(source_file, 'r', encoding='utf-8') as f:
            content = json.load(f)
            if isinstance(content, list):
                merged_list.extend(content)
            else:
                merged_list.append(content)
    return merged_list

def transform_data(data):
    new_data = []
    for item in data:
        metadata_list = [item.get("suite_name"), item.get("case_name"), item.get("cluster")]
        metadata = "_".join(metadata_list)

        row = item.get("row") if item.get("row") else item.get("results", {}).get("count()", 0)
        elapsed_ms = item.get("elapsed_ms") if item.get("elapsed_ms") else item.get("statistics", {}).get("elapsed_ms", 0)
        eps = float(row) / float(elapsed_ms) if elapsed_ms and float(elapsed_ms) != 0 else 0
        eps_format = f"{eps:.3f}"

        new_item = {
            "metadata": metadata,
            "suite_name": item.get("suite_name"),
            "case_name": item.get("case_name"),
            "cluster": item.get("cluster"),
            "version": item.get("version"),
            "row": row,
            "elapsed_ms": elapsed_ms,
            "eps": eps_format
        }
        new_data.append(new_item)
    return new_data

## Compare test result with benchmark
def get_previous_versions(current_version: str):
    version_helper = VersionHelper()
    if SANITIZER == "release" or SANITIZER == "":
        previous = version_helper.get_previous_release_version(current_version)
    else:
        previous = version_helper.get_latest_sanitizer_version(current_version, "nightly_test.yml")
    return previous

def get_compare_benchmark(current_version: str, specified_version: str, benchmark: list):
    previous = get_previous_versions(current_version)
    previous = [next((v for v in previous for item in benchmark if item.get('version') == v), None)] # skip version with empty benchmark
    previous_benchmark = [item for item in benchmark if item.get('version') in previous]
    if len(previous_benchmark) == 0:
        print(f"previous benchmark not found: {current_version}")
        return None, None

    if (SANITIZER == "release" or SANITIZER == "") and specified_version is not None:
        version_helper = VersionHelper()
        specified = version_helper.get_version_list(specified_version)
        specified = [v for v in specified if v not in previous]
        specified_benchmark = [item for item in benchmark if item.get('version') in specified]
        previous_benchmark.extend(specified_benchmark)

    print(f"previous version: {previous}, current version: {current_version}")
    return previous[0], previous_benchmark
   
def calculate_percentage_change(old_value: str, new_value: str) -> float:
    old = float(old_value)
    new = float(new_value)
    diff = new - old
    percentage = (diff / old * 100) if old > 0 else float('inf')
    return percentage

def compare_benchmarks(result_list, benchmark_list, warning_version, threshold):
    warning = 0
    threshold = threshold.replace('%', '').strip()
    for result in result_list:
        for ben in benchmark_list:
            if result.get("metadata") == ben.get("metadata"):           
                bench_key = "benchmark_" + ben.get("version")
                compare_key = "compare_" + ben.get("version")
                compare_key = "compare_" + ben.get("version")
                
                compare = calculate_percentage_change(result.get("eps"), ben.get("eps"))
                result[bench_key] = ben.get("eps")
                result[compare_key] = f"{compare:.2f}%"

                if warning_version == ben.get("version"):
                    if abs(compare) > int(threshold):
                        result["warning"] = 1
                        warning = 1
                    else:
                        result["warning"] = 0

    for result in result_list:
        del result["metadata"]
    return result_list, warning

## Generate file
def generate_file_name(dir, data, key, suffix):
    output_file_name = "_".join([data[0].get("version")[:19], key])
    output_file = os.path.join(dir, f"{output_file_name}.{suffix}")
    return output_file

def generate_json(dir, data, key):
    output_file = generate_file_name(dir, data, key, "json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)

def generate_excel(dir, data, key):
    output_file = generate_file_name(dir, data, key, "xlsx")
    df = pd.DataFrame(data)
    df.to_excel(output_file, index=False)

def generate_markdown(dir, data):
    output_file = os.path.join(dir, f"table.md")

    headers = data[0].keys()

    markdown_table = []
    markdown_table.append('| ' + ' | '.join(headers) + ' |')
    markdown_table.append('|' + '|'.join(['---'] * len(headers)) + '|')

    for row in data:
        markdown_table.append('| ' + ' | '.join(str(row[header]) for header in headers) + ' |')

    with open(output_file, 'w') as file:
        file.write('\n'.join(markdown_table))
        file.write('\n')

    logging.info("Markdown table has been stored in 'table.md' file.")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--versions', type=str, required=False, help='Comma-separated list of versions for comparasion (e.g.1.0.0,1.1.0)')    
    parser.add_argument('--dir', type=str, required=True, default='./', help='Directory of test cases result')
    parser.add_argument('--threshold', type=str, required=False, default='10%', help='Threshold of benchmarking test')
    parser.add_argument('--save-benchmark', action='store_true', help='save test result as benchmark')
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    # load result
    result_path = os.path.join(args.dir, "result")
    result = load_json(result_path)
    result_list = transform_data(result)
    if len(result_list) == 0:
        raise Exception("failed to load test results")

    if args.save_benchmark:
        logging.info("recorad and upload benchmark")
        save_path = os.path.join(args.dir, "benchmark", "new")
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        generate_json(save_path, result_list, "benchmark")

    # load benchmark
    benchmark_path = os.path.join(args.dir, "benchmark")
    benchmark = load_json(benchmark_path)

    current_version = PROTON_VERSION
    warning_version, filtered_benchmark = get_compare_benchmark(current_version, args.versions, benchmark)
    if filtered_benchmark is None:
        raise Exception("failed to load benchmark")

    if warning_version == current_version:
        logging.info(f"current version ({current_version}) same as compare version ({warning_version}), skip comparasion")
        sys.exit(0)

    benchmark_list = transform_data(filtered_benchmark)

    # compare result and benchmark
    output_data, warning = compare_benchmarks(result_list, benchmark_list, warning_version, args.threshold)
    logging.info("compare output:")
    logging.info("---------------")
    logging.info(output_data)
    logging.info("---------------")

    # generate comparasion result
    output_path = os.path.join(args.dir, "summary")
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    generate_json(output_path, output_data, "result")
    generate_excel(output_path, output_data, "result")
    generate_markdown(output_path, output_data)

    if warning != 0:
        raise Exception("performance warning found!")
