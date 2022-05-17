import os, json, time, sys, logging, getopt
import requests
from timeplus import (Env,Stream,StreamColumn, Query)
from rx import operators as ops
from github import (Github,enable_console_debug_logging,GithubException,RateLimitExceededException)
from requests.exceptions import ReadTimeout
import multiprocessing as mp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

query_list = [
    "select * from $stream_name",
    "select * from $stream_name",
    "SELECT window_end AS time,count(*) AS cnt FROM tumble($stream_name,5s) WHERE _tp_time > to_start_of_hour(now()) GROUP BY window_end SETTINGS seek_to='-1h'", 
    "SELECT window_end,repo, group_array(distinct actor) AS watchers FROM hop($stream_name,1m,10m) WHERE type ='WatchEvent' GROUP BY window_end,repo HAVING length(watchers)>1 emit last 1h",
    "SELECT repo, count(*) FROM $stream_name WHERE type ='WatchEvent' group by repo", 
    "SELECT created_at,repo,json_extract_string(payload,'master_branch') AS master_branch FROM $stream_name WHERE type='CreateEvent'",
    "select now(),repo,count(*) as cnt from hop($stream_name,1s,30m) group by repo,window_end having cnt >5 emit last 1h",
    "select now(),type,count(*) from $stream_name group by type",
    "select now(),count(*) from $stream_name",
    "select window_end,type,count() from tumble($stream_name,1h) group by window_end,type emit last 2d"
]


tp_schema = 'http'
tp_host = 'localhost'#172.31.51.184
tp_port = "8000"

WORKERS = 1

def catch_events(github_token):

    events_cached = []
    g = Github(github_token,per_page=100)
    #enable_console_debug_logging()
    try:
        user = g.get_user()
        print(f"Login successfully as {user.login}")
    except GithubException:
        sys.exit("Please set the github personal access token for GITHUB_TOKEN")

    #hacky way to avoid adding duplicated event id. PyGithub doesn't support etag
    known_ids=set()
    for i in range(1):
        try:
            events = g.get_events()
            add_count=0
            for e in events:
                if e.id not in known_ids:
                    known_ids.add(e.id)
                    add_count=add_count+1
                    line = [[e.id,e.created_at,e.actor.login,e.type,e.repo.name,json.dumps(e.payload)]]
                    events_cached.append(line)
            logger.debug(f"cached {add_count} events (ctrl+c to abore)")
            time.sleep(1)       
        except RateLimitExceededException:
            logger.debug("Rate limit exceeded. Sleeping for 10 minutes")
            time.sleep(600)
        except ReadTimeout:
            logger.debug("Connection timed out. Sleeping for 10 minutes")
            time.sleep(600)
        except KeyboardInterrupt:
            logger.debug("Good bye!")
        except Exception as error:
            logger.debug(f"Exception, error = {error}")   
    logger.debug(f"len(events_cached) = {len(events_cached)}")    
    return events_cached 


def create_stream(stream_name, tp_schema, tp_host, tp_port, client_id,client_secret):
    env = (
        Env()
        .schema(tp_schema).host(tp_host).port(tp_port)
        #.login(client_id=client_id, client_secret=client_secret)
    )
    Env.setCurrent(env)

    try:
        s = (
            Stream()
            .name(stream_name)
            .column(StreamColumn().name("id").type("string"))
            .column(StreamColumn().name("created_at").type("datetime"))
            .column(StreamColumn().name("actor").type("string"))
            .column(StreamColumn().name("type").type("string"))
            .column(StreamColumn().name("repo").type("string"))
            .column(StreamColumn().name("payload").type("string"))
            .ttl_expression("created_at + INTERVAL 2 WEEK")
        )
        if(s.get() is None):
            s.create()
            print(f"Created a new stream {s.name()}")
        return stream_name
    except Exception as e:
        sys.exit(f"Failed to list or create data streams from {tp_schema}://{tp_host}:{tp_port}. Please make sure you are connecting to the right server.")    


def git_event_load(stream_name,tp_schema, tp_host, tp_port, client_id,client_secret, events):
    logger = mp.get_logger()     
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)   
    logger.setLevel(logging.DEBUG)     
    logger.debug(f"stream_name = {stream_name}, input starts...")
    env = (
        Env()
        .schema(tp_schema).host(tp_host).port(tp_port)
        #.login(client_id=client_id, client_secret=client_secret)
    )
    Env.setCurrent(env)

    logger.debug(f"input for stream_name = {stream_name}, env setup done")

    try:
        s = (
            Stream()
            .name(stream_name)
            .column(StreamColumn().name("id").type("string"))
            .column(StreamColumn().name("created_at").type("datetime"))
            .column(StreamColumn().name("actor").type("string"))
            .column(StreamColumn().name("type").type("string"))
            .column(StreamColumn().name("repo").type("string"))
            .column(StreamColumn().name("payload").type("string"))
            .ttl_expression("created_at + INTERVAL 2 WEEK")
        )
        if(s.get() is None):
            s.create()
            print(f"Created a new stream {s.name()}")
    except Exception as e:
        sys.exit(f"Failed to list or create data streams from {tp_schema}://{tp_host}:{tp_port}. Please make sure you are connecting to the right server.")       

    i = 0
    while(True):
        try:
            #events = g.get_events()
            add_count = 0
            for e in events:
                add_count=add_count+1
                s.insert(e)    
            if i == 0:
                logger.debug(f"added {add_count} events as 1st batch, continue to insert in an interval of 2 secionds......)")
            time.sleep(1)
            i += 1
        except KeyboardInterrupt:
            print("Good bye!")
            break
        except Exception as error:
            logger.debug(f"Exception, error = {error}")    

def query_handler(line):
    i = 0
    while i < 100:
        print(line)

def raise_exception(error):
    raise Exception (f"Error: {error}")


def update_emit_count(emit_counter_array, emit_count,emit_stats_base, msg): #emit_stats_base logging total emit count per emit_stats_base times counting.
    logger = mp.get_logger()  
    #logger.debug(f"emit_counter_array = {emit_counter_array}, emit_count = {emit_count}")   s
    try:
        total =  emit_counter_array[0] + emit_count
        emit_counter_array[0] = total
        if (total < 10) or (total % emit_stats_base == 0):
            logger.debug(f"total_emit_count = {emit_counter_array[0]}, {msg} ")    
    except Exception as error:
        logger.debug(f"Exception, {error}")

def event_query(tp_schema, tp_host, tp_port, client_id, client_secret, query_str, rx_mode='ops_take', emit_stats_base=10):
    total_emit_count = [0]
    logger = mp.get_logger()     
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)   
    logger.setLevel(logging.DEBUG)     
    logger.debug(f"tp_host = {tp_host}, tp_port = {tp_port}, rx_mode = {rx_mode}, emit_stats_base = {emit_stats_base}, start running with query_str = {query_str}")
    try:
        env = (
            Env()
            .schema(tp_schema).host(tp_host).port(tp_port)
            #.login(client_id=client_id, client_secret=client_secret)
        )
        Env.setCurrent(env)

        logger.debug(f"query_str = {query_str}, env setup done")

        query = Query().sql(query_str)        
        query.create() 
        
        while True:
            #query = Query().sql(query_str)        
            #query.create()
            #query_result = []

            if rx_mode == 'ops_take':
                query.get_result_stream().pipe(ops.take(1000)).subscribe(
                #query.get_result_stream().subscribe(
                    on_next=lambda i: update_emit_count(total_emit_count,1,emit_stats_base,f"rx_mode=ops_take"),
                    on_error=lambda e: raise_exception(f"query.get_result_stream exception:{e}"),
                    on_completed=lambda: query.stop(),
                )
            elif rx_mode == 'no_pipe':
                query.get_result_stream().subscribe(
                #query.get_result_stream().subscribe(
                    on_next=lambda i: update_emit_count(total_emit_count,1,emit_stats_base, f"rx_mode=no_pipe"),
                    on_error=lambda e: raise_exception(f"query.get_result_stream exception:{e}"),
                    on_completed=lambda: query.stop(),
                ) 
    except Exception as e:
        logger.debug(f"Exception, {e}")          



        #logger.debug(f"query_result = {query_result}")
        #query.delete()




if __name__ == "__main__":

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    github_token = os.getenv("GITHUB_TOKEN")
    client_id = os.getenv("GITHUB_CLIENT_ID")
    client_secret = os.getenv("GITHUB_CLIENT_SECRET")
    tp_host = os.getenv("TP_HOST", 'localhost')
    tp_schema = os.getenv("TP_SCHEMA",'http')
    tp_port = os.getenv("TP_PORT",8000)
    input_workers = int(os.getenv("GITHUB_WORKERS", 2))
    query_workers = int(os.getenv("GITHUB_WORKERS", 2))
    stream_name_prefix = 'github_event'
    stream_number = 2
    rx_mode = "ops_take" #by default rx_mode = ops_take: take 1000 lines and then, could be set rx_mode=no_pipe means just keep receiving data but no pipe.
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', ["tp_host=", "tp_port=","tp_schema=", "rx_mode=", "input_workers=", "query_workers=", "emit_stats_base=", "stream_name_prefix=", "stream_number="])
    except(getopt.GetoptError) as error:
        print(f"command error: {error}")
        print(f"usage: python3 perf_run_git.py --rx_mode=no_pipe")
        sys.exit(1)
    print(f"opts = {opts}") 
    for name, value in opts:
        
        if name in ("--rx_mode"):
            if value == None or value == '':
                print(f"usage: python3 perf_run_git.py --rx_mode=no_pipe")
                sys.exit(1)
            else:
                rx_mode = value
        if name in ('--tp_host'):
            tp_host = value

        if name in ('--tp_port'):
            tp_port = value
        if name in ('tp_schema'):
            tp_schema = value 
        if name in ('--input_workers'):
            input_workers = value
        if name in ('--query_workers'):
            query_workers = value
        if name in ('--emit_stats_base'):
            emit_stats_base = int(value)
        if name in ('--stream_name_prefix'):
            stream_name_prefix = value
        if name in ('--stream_number'):
            stream_number = int(value)
              

    input_workers = int(input_workers)
    query_workers = int(query_workers)

    logger.debug(f"github_token = {github_token}, client_id = {client_id}, client_secret = {client_secret}, tp_host = {tp_host}, input_workers = {input_workers}, query_workers = {query_workers}, rx_mode = {rx_mode}, emit_stats_base = {emit_stats_base}, stream_name_prefix = {stream_name_prefix}, stream_number = {stream_number}")
    

    events = catch_events(github_token)


    logger.debug 
    procs = []

    streams = []
    for i in range(stream_number):
        stream_name = f"{stream_name_prefix}_{i}"
        s_name = create_stream(stream_name, tp_schema, tp_host, tp_port, client_id,client_secret)
        streams.append(s_name)

    stream_number = len(streams)

    k = 0#stream counter
    for i in range(input_workers): # creae process to ingest github_event data
        stream_name = streams[k]
        args = (
            stream_name,
            tp_schema,
            tp_host,
            tp_port,
            client_id,
            client_secret,
            events
        )
        proc = mp.Process(target = git_event_load, args = args)
        procs.append(proc)  
        proc.start()
        logger.debug(f"stream_name = {stream_name}, len(events) = {len(events)}, input proc = {proc} started.")        
        k += 1
        if k >= stream_number:
            k = 0        
    

    available_stream_number = min(stream_number, query_workers)
    logger.debug(f'available_stream_number= {available_stream_number}')

    j = 0 # query_list counting, allocate the query to the workers
    k = 0 # strean_name counting, it's input_workers, allocate stream_name to the workers
    for i in range(query_workers): # create stream query to tail from neutron
        query_str = query_list[j]
        query_str = query_str.replace('$stream_name', f'{stream_name_prefix}_{k}')
        #query_str = f"select window_end,type,count() from tumble(github_event{i},1h) group by window_end,type emit last 2d"
        args = ( 
            tp_schema,
            tp_host, 
            tp_port, 
            client_id, 
            client_secret, 
            query_str,
            rx_mode,
            emit_stats_base
        )
        proc = mp.Process(target = event_query, args = args)
        procs.append(proc)
        proc.start()
        j += 1
        k += 1
        if j >= len(query_list):
            j = 0

        if k >= available_stream_number:
            k = 0
        
    for proc in procs:
        proc.join()
    

    
    
    


