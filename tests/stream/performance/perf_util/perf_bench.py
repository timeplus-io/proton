import os, sys, logging, subprocess, time, datetime, json, csv, argparse, getopt, glob
import multiprocessing as mp
import pytest
import pandas as pd
from distutils.version import StrictVersion

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)") 

PROTON_PYTHON_DRIVER_S3_BUCKET_NAME = "tp-internal"
PROTON_PYTHON_DIRVER_S3_OBJ_NAME = "proton/proton-python-driver/clickhouse-driver-0.2.4.tar.gz"
PROTON_PYTHON_DRIVER_FILE_NAME ="clickhouse-driver-0.2.4.tar.gz"
PROTON_PYTHON_DRIVER_NANME = "clickhouse-driver"

NEUTRON_PERF_SUMMARY_FILE_PREFIX = "neutron_perf_summary_report"
NEUTRON_LATENCY_PERF_REPORT_FILE_PATTERN = "*_neutron_*.csv"
NEUTRON_LATENCY_PERF_SUMMARY_FILE_TEMPLATE="neutron_perf_summary_template.csv"
#NEUTRON_LATENCY_PERF_SUMMARY_FILE_PATTERN = "neutron_perf_summary_report*"
'''
end to end perf report naming convension: 
{version}_{component_name}_{type}_{suite}_{timestamp}.csv, e.g. 1.0.42_neutron_latency_middle_1652597055.csv

end to end perf summary report naming convension: 
{component_name}_perf_summary_report_{version}_{timestamp}.csv

neutron_perf_latency.csv format:

type,metrics, diff_latest_percent, diff_baseline_percent, 1.0.42,1.0.36,.....1.0.33
latency,middle_min,num,...
latency,middle_max,num,...
latency,middle_mean,num,...
latency,middle_p90,num,... 
latency,large_min,num,...
latency,large_max,num,...
latency,large_mean,num,...
latency,large_p90,num,...
latency,xlarge_min,num,...
latency,xlarge_max,num,...
latency,xlarge_mean,num,...
latency,xlarge_p90,num,...
throughput,xlarge_min,num,...
throughput,xlarge_max,num,...
throughput,xlarge_mean,num,...
throughput,xlarge_p90,num,...

Note: 
1. diff_latest_percent=(num_1.0.38 - num_1.0.36)/num_1.0.36, diff_baseline_percent=(num_1.0.38 - num_1.0.33)/num_1.0.33
2. perf report name convension: ComponentVersion_ComponentName_Type_Metric_Timestamp.csv, "_" is reserved as a delimeter and can't be used in ComponentName, Type, Metric and .etc

'''

cur_dir = os.path.dirname(os.path.abspath(__file__))


def get_latest_file(file_pattern):
    #search the folder based on the file pattern and get the file with latest modify timestamp
    list_of_files = glob.glob(file_pattern)
    if list_of_files != None and len(list_of_files) > 0:
        latest_file = max(list_of_files, key=os.path.getctime)
    else:
        latest_file = None
    return latest_file
    
def metric_stats(df, metric_type, metric_ver, metric_name_prefix):
    try:
        metrics = [] #min, max, avg, p90
        dirty_rows = []
        for row in df.itertuples():
            if row[-1] > 1000:
                logger.debug(f"row = {row}")
                dirty_rows.append(row.Index)
        logger.debug(f"dirty_rows = {dirty_rows}")
        df = df.drop(dirty_rows)
        metrics_name = [f"{metric_name_prefix}_min", f"{metric_name_prefix}_max", f"{metric_name_prefix}_mean", f"{metric_name_prefix}_p90"]
        metrics.append(df[df.columns[-1]].min())
        metrics.append(df[df.columns[-1]].max())
        metrics.append(df[df.columns[-1]].mean())
        metrics.append(df[df.columns[-1]].quantile(0.9, interpolation='nearest'))
        df_metrics = pd.DataFrame(
            {
                'metrics':metrics_name,
                f"{metric_ver}":metrics
            
            }
        )
        return df_metrics        
    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")

def str_exists_in_df_column(df, column, str):
    #logger.debug(f"column = {column}, str = {str}, df = \n{df}\n")
    if column in df.columns:
        str_count = df[column].str.contains(str).sum()
        if str_count > 0:
            return True
        else:
            return False
    else:
        return False

def update_perf(df_perf_summary, perf_report_path, perf_report):
    perf_report_name_elements = perf_report.split("_")
    metric_ver = perf_report_name_elements[0].replace(perf_report_path, '')
    component_name = perf_report_name_elements[1]
    metric_type = perf_report_name_elements[2]
    metric_name_prefix = perf_report_name_elements[3]
    df_perf_report = pd.read_csv(perf_report, index_col = False)
    df_metrics = metric_stats(df_perf_report,metric_type,metric_ver,metric_name_prefix) # read the report csv and calculated the metrics out based on the report name
    logger.debug(f"df_metrics = \n{df_metrics}")
    cur_dir = cur_dir = os.path.dirname(os.path.abspath(__file__))
    perf_summary_file_template = cur_dir + "/" + NEUTRON_LATENCY_PERF_SUMMARY_FILE_TEMPLATE

    if df_perf_summary is None: # if df_perf_summary is None, create df_perf_summary DataFrame from a template csv
        df_perf_summary = pd.read_csv(perf_summary_file_template, index_col = False)
        logger.debug(f"df_perf_summary is None and create df_perf_summary DataFrame = \n{df_perf_summary}")
    else:
        pass
        logger.debug(f"df_perf_summary gottern from parameter = \n{df_perf_summary}")
    
    
    if not (metric_ver in df_perf_summary.columns): # if no metric_ver exist in the df_perf_summary.columns, insert a column and update related metric rows.
        df_perf_summary.insert(4, metric_ver, 0) # insert a new column with all 0  

    for row in df_metrics.itertuples(): # update the df_perf_summary['metric_ver'] column based on the df_metrics DataFrame out of chameleon perf report
        logger.debug(f"row = {row}") 
        if not str_exists_in_df_column(df_perf_summary, 'metrics', row.metrics): #if row.metrics is not in df_perf_summary.columns, add one row with all 0
            df_perf_summary_new_row = [metric_type, row.metrics] + [0 for i in range(len(df_perf_summary.columns)-2)]
            df_perf_summary.loc[len(df_perf_summary.index)] = df_perf_summary_new_row# add a row with all 0 

        df_perf_summary.loc[df_perf_summary['metrics'] == row.metrics, metric_ver] = row[-1] #update related rows according to the metric_name  

    
    if len(df_perf_summary.columns) >= 6: # calculate the diff and update the df_perf_summary DataFrame
        df_perf_summary['diff_latest_percent'] = (df_perf_summary[df_perf_summary.columns[4]] - df_perf_summary[df_perf_summary.columns[5]])/df_perf_summary[df_perf_summary.columns[5]]
        df_perf_summary['diff_latest_percent'] = df_perf_summary['diff_latest_percent'].round(4)
        df_perf_summary['diff_latest_percent'] = df_perf_summary['diff_latest_percent']*100
        df_perf_summary['diff_baseline_percent'] = (df_perf_summary[df_perf_summary.columns[4]] - df_perf_summary[df_perf_summary.columns[-1]])/df_perf_summary[df_perf_summary.columns[-1]]
        df_perf_summary['diff_baseline_percent'] = df_perf_summary['diff_baseline_percent'].round(4)
        df_perf_summary['diff_baseline_percent'] = df_perf_summary['diff_baseline_percent']*100

    logger.debug(f"df_perf_summary = \n{df_perf_summary}")
    return df_perf_summary
    

def generate_perf_summary_on_path(perf_summary_file_prefix, perf_report_path, perf_report_pattern = NEUTRON_LATENCY_PERF_REPORT_FILE_PATTERN):
    perf_files = []
    #perf_summary_file_pattern = perf_summary_file_prefix + "*" 
    #perf_summary_file = get_latest_file(perf_summary_file_pattern)
    #logger.debug(f"perf_summary_file = get_latest_file({perf_summary_file_pattern}) = {perf_summary_file}")
    perf_summary_file = None # todo: support update mode, update existing file but not recreate a new summary file
    if perf_summary_file != None:
        try:
            df_perf_summary = pd.read_csv(perf_summary_file, index_col = False)
        except:
            print(f"perf_summary_file exists = {perf_summary_file}, but in wrong format.")
            sys.exit(1)
    else:
        df_perf_summary = None    
    
    logger.debug(f"perf_report_path = {perf_report_path}")
    perf_report_pattern = perf_report_path + '/'+ perf_report_pattern
    logger.debug(f"perf_report_pattern = {perf_report_pattern}")
    report_files = glob.glob(perf_report_pattern)
    logger.debug(f"report_files = {report_files}")
    version_list = []
    for report in report_files:
        report_name = report.split("_")
        metric_version = report_name[0].replace(perf_report_path, '')
        version_list.append(metric_version)
    #version_list = list(dict.fromkeys(version_list)) #decup the version_list
    version_list = list(dict.fromkeys(version_list)) #decup the version_list
    version_list.sort(key=StrictVersion)
    logger.debug(f"version_list = {version_list}")
    last_version = 'null'
    for version in version_list:
        files = glob.glob(f"{perf_report_path}/{version}*")
        latest_reports = []
        for file in files: #for the duplicated reports like 1.0.38_neutron_latency_large_165784.csv and 1.0.38_neutron_latency_large_175786.csv, only get the the one with the latest update time into the latest_reports
            file_pattern = '_'.join(file.split('_')[:-1])+'*'
            logger.debug(f"file_pattern of report = {file_pattern}")
            file = get_latest_file(file_pattern)
            if not file in latest_reports:
                latest_reports.append(file)
        logger.debug(f"latest_reports = {latest_reports}")

        for file in latest_reports:
            report_name = file.split("_")
            metric_version = report_name[0].replace(perf_report_path, '')
            component_name = report_name[1]
            metric_type = report_name[2]
            metric_name_prefix = report_name[3]
            perf_report = file
            logger.debug(f"To update df_perf_summary DataFrame by processing perf_report = {perf_report}")
            df_perf_summary = update_perf(df_perf_summary, perf_report_path, perf_report)
            logger.debug(f"df_perf_summary DataFrame updated by processing perf_report = {perf_report}")
        last_version = version
            
    new_perf_summary_file = cur_dir + "/" + NEUTRON_PERF_SUMMARY_FILE_PREFIX + '_' + last_version + '_' + str(datetime.datetime.now(datetime.timezone.utc).timestamp()).split('.')[0] + '.csv'
    df_perf_summary.to_csv(new_perf_summary_file, index = False)



if __name__ == "__main__":
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)    
    

    perf_report_path = "./"
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', ["perf_report_path="])
    except(getopt.GetoptError) as error:
        print(f"command error: {error}")
        print(f"usage: python3 perf_bench.py --perf_report_path=~/chameleon/generator/")
        sys.exit(1)
    print(f"opts = {opts}")
    for name, value in opts:
        
        if name in ("--perf_report_path"):
            if value == None or value == '':
                print(f"usage: python3 perf_bench.py --perf_report_path=~/chameleon/generator")
                sys.exit(1)
            else:
                perf_report_path = value
        
    print(f"perf_bench: perf_report_path = {perf_report_path}")

    generate_perf_summary_on_path(NEUTRON_PERF_SUMMARY_FILE_PREFIX, perf_report_path, perf_report_pattern = NEUTRON_LATENCY_PERF_REPORT_FILE_PATTERN)







