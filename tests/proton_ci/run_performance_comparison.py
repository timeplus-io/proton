import logging
import sys
import argparse
import os
import pandas as pd

from env_helper import PROTON_VERSION, PROTON_REPO
from performance_compare import load_json, transform_data, generate_file_name, generate_json, generate_excel, generate_markdown

if PROTON_VERSION is None:
    logging.error("PROTON_VERSION is None, could not find proton image")
    sys.exit(1)

if PROTON_REPO is None:
    logging.error("PROTON_REPO is None, could not find proton image")
    sys.exit(1)
    
def calculate_percentage_change(new_value: str, old_value: str) -> float:
    old = float(old_value)
    new = float(new_value)
    diff = new - old
    percentage = (diff / old * 100) if old > 0 else float('inf')
    return percentage

def compare_benchmarks(source_result_list, target_result_list, threshold):
    warning = 0
    threshold = threshold.replace('%', '').strip()
    for result in target_result_list:
        for ben in source_result_list:
            if result.get("metadata") == ben.get("metadata"):           
                bench_key = "benchmark_" + ben.get("version")
                compare_key = "compare_" + ben.get("version")
                compare_key = "compare_" + ben.get("version")
                
                compare = calculate_percentage_change(result.get("eps"), ben.get("eps"))
                result[bench_key] = ben.get("eps")
                result[compare_key] = f"{compare:.2f}%"

                if compare > int(threshold):
                    result["warning"] = 1
                    warning = 1
                else:
                    result["warning"] = 0

    for result in target_result_list:
        del result["metadata"]
    return target_result_list, warning

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=str, required=True, default='./', help='Directory of test cases result')
    parser.add_argument('--threshold', type=str, required=False, default='10%', help='Threshold of benchmarking test')
    return parser.parse_args()
    
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    # load source proton result
    source_result_path = os.path.join(args.dir, "source_result")
    source_result = load_json(source_result_path)
    source_result_list = transform_data(source_result)
    if len(source_result_list) == 0:
        logging.warning("failed to load source results")
        sys.exit(1)
        
    # load target proton result
    target_result_path = os.path.join(args.dir, "target_result")
    target_result = load_json(target_result_path)
    target_result_list = transform_data(target_result)
    if len(target_result_list) == 0:
        logging.warning("failed to load target results")
        sys.exit(1)
        
    # compare source and target
    output_data, warning = compare_benchmarks(source_result_list, target_result_list, args.threshold)
    logging.info("compare output:")
    logging.info("---------------")
    logging.info(output_data)
    logging.info("---------------")
    
    # generate comparasion result
    output_path = os.path.join(args.dir, "summary")
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    generate_json(output_path, output_data, "target_result")
    generate_excel(output_path, output_data, "target_result")
    generate_markdown(output_path, output_data)

    if warning != 0:
        logging.warning("performance warning found!")
        sys.exit(1)
