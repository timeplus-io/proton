import os, sys, json, getopt, subprocess, traceback
import logging, logging.config
import time
import datetime
import random
import requests
import csv
import multiprocessing as mp
from proton_driver import Client
from proton_driver import errors
from github import (Github,enable_console_debug_logging,GithubException,RateLimitExceededException)
from requests.exceptions import ReadTimeout
import requests
import pandas as pd

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)




def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(round(P * len(N) + 0.5))
    return N[n-1]


def dict_to_jsonstr(string):
    json_str = json.dumps(string)
    json_str = json_str.replace("\\", "\\\\").replace("'","\\'")
    #json_str = json_str.replace(r"\", r"\\").replace(r"'",r"\'")
    return json_str     

def batch_json_input(json_batch, stream, target_columns, num=0, interval=1, ingest_mode='split'):
    json_batch_len = len(json_batch) if num==0 else num
    logger.debug(f"json_batch_len = {json_batch_len}")
    client = Client('localhost',port = 8463)
    #json_str = json.dumps(json_batch[0])
    add_count = 0
    i = 0
    type_str = 'PullRequestReviewCommentEvent'
    column_length_at_input = []

    if ingest_mode != 'split':
        json_batch_str = str(json_batch)
        json_str = dict_to_jsonstr(json_batch)
        json_str_len = len(json_str)
        column_length_at_input.append(json_str_len)
        add_count = add_count + 1
        sent_at = datetime.datetime.utcnow()
        insert_sql = f"insert into {stream} (add_count, type," 
        for column in target_columns:
            insert_sql = insert_sql + column + ','
        insert_sql = f"{insert_sql} sent_at) values({add_count},'{type_str}',"
        for column in target_columns:
            insert_sql = f"{insert_sql} '{json_str}',"
        insert_sql = f"{insert_sql} '{sent_at}')" 
        res = client.execute(insert_sql)
        i += 1
        logger.debug(f"add_count = {add_count}, insert_sql = {insert_sql} \n done. \n")
        time.sleep(interval)
    else:               

        while i < json_batch_len:
            item = json_batch[i]
            json_str = dict_to_jsonstr(item)
            json_str_len = len(json_str)
            column_length_at_input.append(json_str_len)
            add_count = add_count + 1
            sent_at = datetime.datetime.utcnow()
            insert_sql = f"insert into {stream} (add_count, type," 
            for column in target_columns:
                insert_sql = insert_sql + column + ','
            insert_sql = f"{insert_sql} sent_at) values({add_count},'{type_str}',"
            for column in target_columns:
                insert_sql = f"{insert_sql} '{json_str}',"
            insert_sql = f"{insert_sql} '{sent_at}')"

            #insert_sql = insert_sql.replace("\\", "\\\\").replace("'","\\'")
            res = client.execute(insert_sql)
            i += 1
            logger.debug(f"add_count = {add_count}, insert_sql = {insert_sql}, \n done. \n")
            time.sleep(interval)
    return column_length_at_input
    client.disconnect()

def query_run(stream, query_column, query = None, query_id = '101', query_log_name = 'query_log'):
    pd.set_option('display.precision', 2)
    logger = mp.get_logger()     
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)   
    logger.setLevel(logging.DEBUG) 
    if query_column == None:
        logger.debug(f"query_column is None, exist")
        sys.exit(1)
    else:
        logger.debug(f"query_column = {query_column}")    
    #client = Client('localhost', port = 8463)

    recieve_count = 0
    if query == None:    
        url = f"http://localhost:3218/?query=select sent_at, now64(3) as recvd_at, {query_column} from {stream}"
    else:
        #query_sql = query
        url = f"http://localhost:3218/?query=" + query
    logger.debug(f"url = {url}")
    #query_result_iter = client.execute_iter(
    #    query_sql, with_column_types=True, query_id=query_id
    #)
    query_records = []
    s = requests.Session()
    s.stream = True 
    res = s.get(url, stream=True)
    
    for i in res.iter_lines():
        i = i.decode('utf-8')
        #logger.debug(f"i = {i}")
        line = i.split("\t")
        query_records.append(line)
        #print(f"record gotten from res.iter_lines() = {i}")

    query_log_csv = query_log_name + "-"+str(datetime.datetime.now())+".csv"
    query_records_m = []

    header = ["sent_at", "recvd_at", "latency_ms", "query_column_len"]
    sent_at_list = []
    recvd_at_list = []
    latency_ms_list = []
    query_column_len_list = []
    #latency_list = []
    query_records_m.append(header)
    for item in query_records:
        line = item[:-1]
        if len(line) >= 2:
            recvd_at_str = str(line[1])
            recvd_at_list.append(recvd_at_str)
            sent_at_str = str(line[0])
            sent_at_list.append(sent_at_str)
            t_recvd_at = datetime.datetime.strptime(recvd_at_str,'%Y-%m-%d %H:%M:%S.%f')
            t_sent_at = datetime.datetime.strptime(sent_at_str,'%Y-%m-%d %H:%M:%S.%f')
            diff = (t_recvd_at - t_sent_at).microseconds/1000
            latency_ms_list.append(diff)
            line.append(diff)
            query_column_len = len(item[-1])
            query_column_len_list.append(query_column_len)
            line.append(query_column_len)
            logger.debug(f"sent_at = {line[0]}, recvd_at = {line[1]}, query_column_len = {query_column_len}, latency = {line[2]}")
            query_records_m.append(line) 
    
    total_latency = 0
    '''
    count = 0
    for i in latency_ms_list:
        total_latency += i
        count += 1
    if count > 0:
        avg_latency = total_latency/count
    max_latency = max(latency_ms_list)
    min_latency = min(latency_ms_list)
    '''
    df = pd.DataFrame(
        {
            'sent_at':sent_at_list, 
            'recvd_at':recvd_at_list,
            'query_column_len':query_column_len_list,
            'latency_ms':latency_ms_list,


        }
    )

    #p90_latency = percentile(latency__ms_list, 0.9)
    max_latency = df['latency_ms'].max()
    min_latency = df['latency_ms'].min()
    mean_latency = df['latency_ms'].mean()
    p90_latency = df['latency_ms'].quantile(0.9, interpolation='nearest')
    metrics_latency = [min_latency, max_latency, mean_latency, p90_latency]
    max_column_length = df['query_column_len'].max()
    min_column_length = df['query_column_len'].min()
    mean_column_length = df['query_column_len'].mean()
    p90_column_length = df['query_column_len'].quantile(0.9, interpolation='nearest')

    #logger.debug(f"max_latency = {max_latency}ms, min_latency = {min_latency}ms,  mean_latency = {mean_latency}ms, p90 latency = {p90_latency}")
    metrics_name = ['min', 'max', 'mean', 'p90']
    metrics_column_length = [min_column_length, max_column_length, mean_column_length, p90_column_length]
    df_stats = pd.DataFrame(
        {
            'stats':metrics_name,
            f"query_column={query_column}:latency":metrics_latency,
            f"query_column={query_column}:query_column_length": metrics_column_length
        }
    )
    
    
    print(f"\n{df_stats}\n")    

    with open(query_log_csv, "w") as f:
        writer = csv.writer(f)
        for item in query_records_m:
            writer.writerow(item)
    return query_records
    
def table_exist_py(pyclient, table_name):
    sql_2_run = "show streams"
    try:
        res = pyclient.execute(sql_2_run)
        for element in res:
            if table_name in element:
                return True
        return False
    except (BaseException) as error:
        logger.info(f"exception, error = {error}")
        return False

def query_id_exists_py(py_client, query_id, query_exist_check_sql=None):
    logger = mp.get_logger()
    query_id = str(query_id)
    if query_exist_check_sql == None:
        query_exist_check_sql = "select query_id from system.processes"
    try:
        # logger.debug(f"query_exist_check_sql = {query_exist_check_sql} to be called.")
        res_check_query_id = py_client.execute(query_exist_check_sql)
        logger.debug(f"query_exist_check_sql = {query_exist_check_sql} to was called.")
        logger.debug(f"res_check_query_id = {res_check_query_id}")
        if res_check_query_id != None and isinstance(res_check_query_id, list):
            for element in res_check_query_id:
                logger.debug(f"element = {element}, query_id = {query_id}")
                if query_id in element:
                    return True
            return False
        else:
            logger.debug(f"query_id_list is None or not a list")
            return False
    except (BaseException) as error:
        logger.debug(f"exception, error = {error}")
    return False


def github_event_get(stream = "github_event", interval = 5):
    g = Github(github_token,per_page=100)
    #enable_console_debug_logging()
    try:
        user = g.get_user()
        print(f"Login successfully as {user.login}")
    except GithubException:
        sys.exit("Please set the github personal access token for GITHUB_TOKEN")
    client = Client('localhost',port = 8463)
    known_ids=set()
    while(True):
        try:
            events = g.get_events()
            add_count=0
            for e in events:
                if e.id not in known_ids:
                    known_ids.add(e.id)
                    add_count=add_count+1
                    #s.insert([[e.id,e.created_at,e.actor.login,e.type,e.repo.name,json.dumps(e.payload)]])
                    #print(f"e = {e}")
                    #event_str = e.replace("\\", "\\\\").replace("'","\\'")
                    id_str = e.id.replace("\\", "\\\\").replace("'","\\'")
                    actor_str = e.actor.login.replace("\\", "\\\\").replace("'","\\'")
                    type_str = e.type.replace("\\", "\\\\").replace("'","\\'")
                    repo_str = e.repo.name.replace("\\", "\\\\").replace("'","\\'")
                    payload_str = json.dumps(e.payload)
                    payload_str = payload_str.replace("\\", "\\\\").replace("'","\\'")
                    insert_sql = f"insert into {stream} (add_count, id, created_at, actor, type, repo, payload) values ({add_count}, '{id_str}', '{e.created_at}', '{actor_str}', '{e.type}', '{repo_str}', '{payload_str}')"
                    print(f"add_count = {add_count}, insert_sql = {insert_sql}")
                    res = client.execute(insert_sql)
                    time.sleep(interval)
            print(f"added {add_count} events, skipped {events.totalCount-add_count} duplicate ones. Waiting 2 seconds to fetch again (ctrl+c to abore)")
            time.sleep(2)
        except RateLimitExceededException:
            print("Rate limit exceeded. Sleeping for 10 minutes")
            time.sleep(600)
        except ReadTimeout:
            print("Connection timed out. Sleeping for 10 minutes")
            time.sleep(600)
        except KeyboardInterrupt:
            print("Good bye!")
            break    

def create_stream (stream_name, target_columns, target_columns_types, host = 'localhost', port = 8463, logstore_replica_factor = 1 ): #todo: consolidate stream create logic into one func
    #create stream if not exists {stream} (add_count int,type string, json_string string, {json_column} json, sent_at datetime64)')
    client = Client(host, port=port)
    sql = f'create stream if not exists {stream_name} (add_count int,type string,'
    column_tuple_list = zip(target_columns, target_columns_types)
    for column, type in column_tuple_list:
        sql = sql + f"{column} {type},"
    sql = sql + 'sent_at datetime64)'
    if logstore_replica_factor != 1:
        sql = sql + f"settings logstore_replication_factor={logstore_replica_factor}"
    logger.debug(f"create stream sql = {sql}")
    try:
        client.execute(sql)
        logger.debug(f"stream = {stream} created")
        res = client.execute('show streams')
        logger.debug(f"show streams = {res}")
        return stream_name
    except Exception as error:
        logger.debug(f"create stream exception, {error}")
        return None
    


if __name__ == "__main__":
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    json_files = []
    stream = ''
    interval = 1
    json_batch = []
    json_column = "json"
    target_columns = []
    target_columns_types = []
    query_column = None
    mode = 'input'
    proc = None
    loop = 1
    logstore_replica_factor = 1
    ingest_mode = 'split' #split means take the json obj from the list in the json file one by one and intest, single-ling means all the content in the json file is ingested as one line.
    github_token = os.getenv("GITHUB_TOKEN", 'None')
    proton_host = os.getenv("PROTON_HOST",'localhost')
    proton_port = os.getenv("PROTON_PORT",8463)


    if github_token is None:
        print(f"please config env var of GITHUB_TOKEN")
        sys.exit(1)
    else:
        logger.debug(f"GITHUB_TOKEN env var is detected")

    pd.set_option('display.precision', 2)
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', ["input_json_files=","mode=", "stream=","target_columns=", "target_columns_types=", "query_column=", "interval=", "loop=", "ingest_mode=", "logstore_replica=", "logstore_retension_ms"])
    except(getopt.GetoptError) as error:
        print(f"command error: {error}")
        print(f"usage: python3 json_input.py --input_json_files=github_issue.json --stream=github_issue --target_columns=event,json_string --target_columns_types=json, string, --interval=1 --loop=-1, --ingest_mode=split, --logstore_replica=3, --logstore_retension_ms=480000")
        sys.exit(1)
    print(f"opts = {opts}")
    for name, value in opts:
        
        if name in ("--input_json_files"):
            if value == None or value == '':
                print(f"usage: python3 json_input.py --json=github_issue.json")
                sys.exit(1)
            else:
                json_files = value.split(",")
        
        if name in ("--stream"):
            #os.environ["PROTON_TEST_SUITES"] = value
            stream = value
        if name in ("--mode"):
            mode = value

        if name in ("--target_columns"):
            #os.environ["PROTON_TEST_SUITES"] = value
            target_columns = value.split(",")
        if name in ("--target_columns_types"):
            target_columns_types = value.split(",") 

        if name in ("--query_column"):
            query_column = value 

        if name in ("--interval"):
            if value.isdigit() == False:
                print(f"usage: python3 json_input.py --interval=1")
                sys.exit(1)
            else:
                interval = int(value)              

        if name in ("--loop"):
            try:
                loop = int(value)                 
            except:
                print(f"unknown input for loop, usage: python3 json_input.py --loop=-1")
                sys.exit(1)
        if name in ("--ingest_mode"):
            ingest_mode = value
        
        if name in ("--logstore_replica"):
            logstore_replica_factor = value
        
        if name in ("--logstore_retension_ms"):
            logstore_retension_ms = value
    
    print(f"input_json: input_json_files = {json_files}, stream = {stream}, target_columns = {target_columns}, target_columns_types = {target_columns_types}, query_column = {query_column}, mode = {mode} interval={interval}, ingest_mode = {ingest_mode}")


    client = Client(proton_host, port = proton_port)

    if mode == 'latency':
        client.execute(f"drop stream if exists {stream}")
        time.sleep(1)

    stream = create_stream(stream,target_columns, target_columns_types, proton_host, proton_port, logstore_replica_factor)
    if stream is None:
        logger.debug(f"stream creation failure")
        sys.exit(1)

    if mode == 'latency':
        #client.execute(f'create stream if not exists {stream} (add_count int, type string, {json_column} json, sent_at datetime64)')
        #if ingest_mode == 'both':
        #    client.execute(f'create stream if not exists {stream} (add_count int,type string, json_string string, {json_column} json, sent_at datetime64)')
        #elif ingest_mode == 'json_obj_only':
        #    client.execute(f'create stream if not exists {stream} (add_count int,type string, {json_column} json, sent_at datetime64)')
        #elif ingest_mode == 'json_string_only':
        #    client.execute(f'create stream if not exists {stream} (add_count int,type string, json_string string, sent_at datetime64)')
        #else:
        #    logger.debug(f"ingest_mode = {ingest_mode}, unknown ingest_mode and exit")
        #    sys.exit(1)

        with open(json_files[0]) as f:  #rest interface will have a bad serialization exception if the stream is empty and then start the query and ingestion, so insert one before running the latency test 
            json_batch = json.load(f, strict=False)        
        res = batch_json_input(json_batch, stream, target_columns, 1, interval, ingest_mode)
    
    while not (table_exist_py(client, stream)):
        time.sleepl(1)    
    time.sleep(2)
    
    if mode == "latency":
        query_id = '101'
        args = (stream, query_column, None, query_id, 'query_log') 
        proc = mp.Process(target = query_run, args = args)
        proc.start()
        logger.debug(f"query_run proc started...")
        time.sleep(6)
    column_length_at_input = []    
    if json_files != None:
        for json_file in json_files:
            while not (table_exist_py(client, stream)):
                time.sleepl(1)
            #if mode == 'latency':
            #    while not query_id_exists_py(client, query_id, query_exist_check_sql=None):
            #        time.sleep(1)
            with open(json_file) as f:
                json_batch = json.load(f, strict=False)
            start_at = datetime.datetime.utcnow()
            logger.debug(f"start bach_json_input from {json_file} to {stream}")
            if loop < 0:
                while True:
                    column_length_at_input = batch_json_input(json_batch, stream, target_columns, 0, interval, ingest_mode)
            else:
                i = 0
                while i < loop:
                    column_length_at_input = batch_json_input(json_batch, stream, target_columns, 0, interval, ingest_mode)
                    i += 1
            end_at = datetime.datetime.utcnow()
            logger.debug(f"end batch_json_input of {json_file} to {stream}") 
            duration = end_at - start_at
            logger.debug(f"batch_json_input of {json_file} takes {duration.microseconds/1000} ms")

        if mode == 'latency':
            query_id_chk_sql = f"select query_id from system.processes where multi_search_any(query, ['from event']) and not multi_search_any(query, ['system.processes'])"
            res = client.execute(query_id_chk_sql)
    
            for query_id_list in res:
                for query_id in query_id_list:
                    logger.debug(f"query_id = {query_id}")
                    kill_sql = f"kill query where query_id = '{query_id}'"
                    res = client.execute(kill_sql)
                    while len(res) != 0:
                        res = client.execute("kill query where query_id = '{query_id}'")
                        logger.debug(f"client.execute(kill query where query_id = {query_id}) = {res}")
            

    if proc != None:
        proc.join()   
    
    if len(column_length_at_input) > 0:
        df = pd.DataFrame(column_length_at_input)
        metrics_name = ["min", "max", "mean", "p90"]
        metrics = []
        metrics.append(df[df.columns[-1]].min())
        metrics.append(df[df.columns[-1]].max())
        metrics.append(df[df.columns[-1]].mean())
        metrics.append(df[df.columns[-1]].quantile(0.9, interpolation='nearest'))
        df_metrics = pd.DataFrame(
            {
                'metrics':metrics_name,
                'json_string_column_len_at_input':metrics
            
            }
        )
        print(f"\n{df_metrics}\n")         
        
