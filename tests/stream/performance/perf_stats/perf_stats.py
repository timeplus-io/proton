import os, sys, logging, datetime, getopt, glob
import pandas as pd

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)") 


cur_dir = os.path.dirname(os.path.abspath(__file__))


def update_perf_summary(df_perf_summary, df_metrics, metric_ver):
    for row in df_metrics.itertuples(): # update the df_perf_summary['metric_ver'] column based on the df_metrics DataFrame out of chameleon perf report
        logger.debug(f"row = {row}") 
        df_perf_summary.loc[df_perf_summary['metrics'] == row.metrics, metric_ver] = row[-1] #update related rows according to the metric_name  

    
    if len(df_perf_summary.columns) >= 6: # calculate the diff and update the df_perf_summary DataFrame
        if not df_perf_summary['materialize'].isna().any():
            df_perf_summary['diff_t+_materialize_percent'] = (df_perf_summary[df_perf_summary.columns[-1]] - df_perf_summary[df_perf_summary.columns[3]])/df_perf_summary[df_perf_summary.columns[3]]
            df_perf_summary['diff_t+_materialize_percent'] = df_perf_summary['diff_latest_percent'].round(4)
            df_perf_summary['diff_t+_materialize_percent'] = df_perf_summary['diff_latest_percent']*100
        if not df_perf_summary['splunk'].isna().any():            
            df_perf_summary['diff_t+_splunk_percent'] = (df_perf_summary[df_perf_summary.columns[4]] - df_perf_summary[df_perf_summary.columns[-1]])/df_perf_summary[df_perf_summary.columns[-1]]
            df_perf_summary['diff_t+_splunk_percent'] = df_perf_summary['diff_baseline_percent'].round(4)
            df_perf_summary['diff_t+_splunk_percent'] = df_perf_summary['diff_baseline_percent']*100

    logger.debug(f"df_perf_summary = \n{df_perf_summary}")
    return df_perf_summary    


def metric_stats(df, metric_ver, metric_name_prefix):
    try:
        metrics = [] #min, max, avg, p90
        dirty_rows = []
        '''
        for row in df.itertuples():
            if row[-1] > 10000000:
                logger.debug(f"row = {row}")
                dirty_rows.append(row.Index)
        logger.debug(f"dirty_rows = {dirty_rows}")
        df = df.drop(dirty_rows)
        '''
        metrics_name = [f"{metric_name_prefix}_min", f"{metric_name_prefix}_max", f"{metric_name_prefix}_mean", f"{metric_name_prefix}_p90"]
        metrics.append(df[df.columns[-1]].min().round(2))
        metrics.append(df[df.columns[-1]].max().round(2))
        metrics.append(df[df.columns[-1]].mean().round(2))
        metrics.append(df[df.columns[-1]].quantile(0.9, interpolation='nearest').round(2))
        df_metrics = pd.DataFrame(
            {
                'metrics':metrics_name,
                f"{metric_ver}":metrics
            
            }
        )
        return df_metrics        
    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")


def scan_2_summary(file_pattern, metric_type):
    df_perf_summary = pd.DataFrame(columns = ['metrics','diff_t+_materialize_percent','diff_t+_splunk_percent', 'materialize', 'splunk', 'timeplus'])
    df_perf_summary['metrics'] = ['small_min', 'small_max', 'small_mean', 'small_p90', 'medium_min','medium_max','medium_mean','medium_p90','large_min','large_max','large_mean','large_p90']    
    files = glob.glob(file_pattern)
    logger.debug(f"file_pattern = {file_pattern}, metric_type={metric_type}, files = {files}")
    for file in files:
        df_report = pd.read_csv(file)
        if 'splunk' in file:
            metric_ver = 'splunk'
        elif 'neutron' in file:
            metric_ver = 'timeplus'
        elif 'materialize' in file:
            metric_ver = 'materialize'
        
        if 'small' in file:
            size = 'small'
        elif 'medium' in file:
            size = 'medium'
        elif 'large' in file:
            size = 'large'    
        df_stats = metric_stats(df_report, metric_ver, size)
        #df_stats_file = f"{perf_report_path}/{size}_{metric_ver}_{metric_name_prefix}_stats_" + str(datetime.datetime.now(datetime.timezone.utc).timestamp()).split('.')[0] + '.csv'
        #df_stats.to_csv(df_stats_file, index = False)
        df_perf_summary = update_perf_summary(df_perf_summary, df_stats, metric_ver)
    
    df_perf_summary_file = f"{perf_report_path}/{metric_type}_stats_" + str(datetime.datetime.now(datetime.timezone.utc).timestamp()).split('.')[0] + '.csv'
    
    print(f"df_perf_summary_file = {df_perf_summary_file}")

    df_perf_summary.to_csv(df_perf_summary_file, index=False) 
    
    return df_perf_summary    

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
    



    latency_file_pattern = f"{perf_report_path}/*latency*report*.csv"
    scan_2_summary(latency_file_pattern, 'latency')

    throughput_file_pattern = f"{perf_report_path}/*throughput*report*.csv"
    scan_2_summary(throughput_file_pattern, 'throughput')
     
    











