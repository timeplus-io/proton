import os, argparse, subprocess, datetime, time
from proton_driver import Client as PTClient
from clickhouse_driver import Client as CHClient
import pandas as pd

SYNC_CMD = "sudo sync"
CLEAR_CACHE_CMD = "sudo tee /proc/sys/vm/drop_caches <<< 1"



class SUT():
    def __init__(self, sut_folder, host, port, client):
        sut_folder_path = sut_folder
        self.sut_folder_path = sut_folder_path
        if sut_folder_path[-1] == '/':
            sut_folder_path = sut_folder_path[:-1]
        self.sut_folder_name = sut_folder_path.split('/')[-1]
        self.host = host
        self.port = port
        self.benmark_queries = self._scan_sut_folder(sut_folder)
        self.client = client #client to connect to the Database for execute sqls

    def _read_sql_from_file(self, file_path):
        try:
            fd = open(file_path, 'r')
            sqlFile = fd.read()
            fd.close()
            sql_commands = sqlFile.split(';')
            if sql_commands[-1] == '/n':
                sql_commands = sql_commands[:-1]
            return sql_commands
        except (BaseException) as error:
            print(f"_read_sql_from_files exception, error: {error}")

    def _scan_sut_folder(self, sut_folder_path):
        try:
            if sut_folder_path is not None and sut_folder_path[-1] == '/':
                sut_folder_path = sut_folder_path[:-1]
            query_path = f"{sut_folder_path}/query"
            sql_commands_dict_list  = []
            query_files = os.listdir(query_path)
            for query_file in query_files:
                query_file = f"{query_path}/{query_file}"
                sql_id = query_file.split('/')[-1]
                sql_commands = self._read_sql_from_file(query_file)
                sql_commands_dict = {'sql_command_id': sql_id, 'sql_commands': sql_commands}
                sql_commands_dict_list.append(sql_commands_dict)
            self.sql_commands_dict_list = sql_commands_dict_list
            return sql_commands_dict_list

        except (BaseException) as error:
            print(f"_scan_sut_folder exception, error: {error}")

    def _exc_cmd(self,cmd):
        print(f"cmd = {cmd}")
        try:
            res = subprocess.run(cmd, shell = True, executable='/bin/bash')
            print(f'res:{res}')
        except(subprocess.CalledProcessError) as error:
            print(f'returncode: {error.returncode}')
            print(f'cmd: {error.cmd}')
            print(f'output: {error.output}')    
    
    def _clear_cache(self, host, ssh_pem_path, ssh_user_name = 'ubuntu'): #todo: support more methods of clear chache
        if host == 'localhost':
            self._exc_cmd(SYNC_CMD)
            self._exc_cmd(CLEAR_CACHE_CMD)
        else:
            sync_cmd = f'ssh -i {ssh_pem_path} {ssh_user_name}@{host} "{SYNC_CMD}"'
            self._exc_cmd(sync_cmd)
            clear_page_cache_cmd = f'ssh -i {ssh_pem_path} {ssh_user_name}@{host} "{CLEAR_CACHE_CMD}"'
            self._exc_cmd(clear_page_cache_cmd)

    def metric_stats(self, data_list, metric_name):
        df_record = pd.DataFrame(data_list, columns=[f"{metric_name}"])
        try:
            metrics = [] #min, max, avg, p90
            metrics_name = [f"min", f"max", f"mean", f"p90"]
            metrics.append(df_record[df_record.columns[-1]].min())
            metrics.append(df_record[df_record.columns[-1]].max())
            metrics.append(df_record[df_record.columns[-1]].mean())
            metrics.append(df_record[df_record.columns[-1]].quantile(0.9, interpolation='nearest'))
            df_metric_stats = pd.DataFrame(
                {
                    'metrics':metrics_name,
                    f"{metric_name}":metrics
                
                }
            )
            return {'record': df_record, 'metric_stats': df_metric_stats}        
        except (BaseException) as error:
            print(f"metrics_stats exception, error = {error}")    

    def _sql_run(self, sql_commands): #sql_commands, a list of sqls
        try:
            start_at = datetime.datetime.now()
            for sql in sql_commands:
                self.client.execute(sql)
            end_at = datetime.datetime.now()
            duration = end_at - start_at
            duration_ms = duration.total_seconds()*1000
            return duration_ms            
        except (BaseException) as error:
            print("_sql_run excdption: {error}")
            #return duration_ms = None
        

    def sql_run_cold(self,sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name):
        print(f"sql_run_cold, sql_id = {sql_id}")
        result = []
        for i in range(rounds):
            self._clear_cache(self.host, ssh_pem_path, ssh_user_name)
            duration_ms = 0
            duration_ms = self._sql_run(sql_commands)
            result.append(duration_ms)
        sql_cold_run_result_dict = self.metric_stats(result, f'{sql_id}_cold_run_response_time')
        return sql_cold_run_result_dict
    
    def sql_run_warm(self, sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name):
        print(f"sql_run_warm, sql_id = {sql_id}")
        result = []
        self._clear_cache(self.host, ssh_pem_path, ssh_user_name)
        self._sql_run(sql_commands) #warm up
        for i in range(rounds):
            duration_ms = self._sql_run(sql_commands)
            result.append(duration_ms)
        sql_warm_run_result_dict = self.metric_stats(result, f'{sql_id}_warm_run_response_time')
        return sql_warm_run_result_dict
    
    def _data_df_to_file(self, data_df, file_path):
        data_df.to_csv(file_path, index = False)
        print(data_df)

    def sql_run_all(self, sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name):
        sql_cold_run_result_dict = self.sql_run_cold(sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name)
        sql_cold_run_record_df = sql_cold_run_result_dict.get("record")
        sql_cold_run_metric_stats_df = sql_cold_run_result_dict.get('metric_stats')
        sql_warm_run_result_dict = self.sql_run_warm(sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name)
        sql_warm_run_record_df = sql_warm_run_result_dict.get('record')
        sql_warm_run_metric_stats_df = sql_warm_run_result_dict.get('metric_stats')
        sql_run_record_df = pd.concat([sql_cold_run_record_df, sql_warm_run_record_df], axis=1)
        sql_run_metric_stats_df = pd.concat([sql_cold_run_metric_stats_df, sql_warm_run_metric_stats_df[sql_warm_run_metric_stats_df.columns[-1]]], axis=1)
        print(f"sql_run_metric_stats_df = {sql_run_metric_stats_df}")
        return {'record':sql_run_record_df, 'metric_stats': sql_run_metric_stats_df}

    
    def benchmark_run(self, mode, rounds, ssh_pem_path, ssh_user_name, report_dir = "./"):
        total_record_df = pd.DataFrame()
        total_metric_stats_df = pd.DataFrame()
        sql_commands_dict_list = self.sql_commands_dict_list
        i = 0
        for sql_commands_dict in sql_commands_dict_list:
            sql_id = sql_commands_dict.get('sql_command_id')
            sql_commands = sql_commands_dict.get('sql_commands')
            if mode == 'all':
                sql_run_result_df_dict = self.sql_run_all(sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name)
            elif mode == 'cold':
                sql_run_result_df_dict = self.sql_run_cold(sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name)
            elif mode == 'warm':
                sql_run_result_df_dict = self.sql_run_warm(sql_id, sql_commands, rounds, ssh_pem_path, ssh_user_name)
            record_df = sql_run_result_df_dict.get('record')
            metric_stats_df = sql_run_result_df_dict.get('metric_stats')
            total_record_df = pd.concat([total_record_df, record_df], axis=1)
            if i == 0:
                total_metric_stats_df = pd.concat([total_metric_stats_df, metric_stats_df], axis=1)
            else:
                for i in range(1, len(record_df.columns)+1):
                    total_metric_stats_df = pd.concat([total_metric_stats_df, metric_stats_df[metric_stats_df.columns[i]]], axis=1)
            i += 1
        if report_dir[-1] != '/':
            report_dir = report_dir + '/'
        record_file_path = report_dir + self.sut_folder_name + '_' + mode + '_' 'record' + '_' + str(int(round(datetime.datetime.now().timestamp()))) + '.csv'
        self._data_df_to_file(total_record_df, record_file_path)
        print(total_record_df)
        metric_stats_file_path = report_dir + self.sut_folder_name + '_' + mode + '_' 'metric_stats' + '_' + str(int(round(datetime.datetime.now().timestamp()))) + '.csv'
        self._data_df_to_file(total_metric_stats_df, metric_stats_file_path)
        print("\n")
        print(total_metric_stats_df)
        






if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("sut_folder", help="the system under test folder contains ddl for creating tables, queries to be loaded and etc.")
    parser.add_argument("-m", "--mode", required=True, help="benchmark running mode, all, code_run, warm_run", default = 'all')
    parser.add_argument("-s", "--server", help="sut server ipaddress/host name, optional, default:localhost", default = 'localhost')
    parser.add_argument("-p", "--port", help="sut server port, default:8463", default = 8463)
    parser.add_argument("-o", "--output",  help="test result ouput, sut_folder_name_record_timestamp.csv, sut_folder_name_summary_timestamp.csv will be created under local mode by default, result could be writen to proton server specified by --output=https://latest.timeplus.io", default = 'local') #todo: remote report to timeplus 
    parser.add_argument("-r", "--rounds", help = "how many rounds to run", default = 10)
    parser.add_argument("-k", "--ssh_pem_path", help = "ssh key path, remote ssh to clear page chage for the code run")
    parser.add_argument("-u", "--ssh_user_name", help = "ssh user name", default = 'ubuntu')
    args = parser.parse_args()
    print(f"args = {args}")
    sut_name = args.sut_folder.split('/')[-1]
    port = int(args.port)
    if 'clickhouse' in args.sut_folder:
        client = CHClient(args.server, port = args.port)
    elif 'timeplus' in args.sut_folder or 'proton' in args.sut_folder:
        client = PTClient(args.server, port = args.port)

    sut = SUT(args.sut_folder, args.server, args.port, client)
    sut.benchmark_run(args.mode, int(args.rounds), args.ssh_pem_path, args.ssh_user_name)
    



    