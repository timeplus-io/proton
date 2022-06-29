# perf_git_run
multiple process runner for ingest github event sample data
Usage example:
export GITHUB_TOKEN=.....
python3 ./perf_run_git.py --rx_mode=no_pipe --tp_host=localhost  --tp_port=8000 --input_workers=40 --query_workers=40 --emit_stats_base=100 --stream_name_prefix=github --stream_number=10
--stream_name_prefix: name prefix of the stream to be created, e.g. github and then the script will auto generate the stream as github_0, github_1 and etc.
--stream_number: how many streams to be created
--input_workers: how many input processes to be created to ingest data into the streams created, if input_worker > stream_number, multiple input worker will write into one stream and
--query_workers: how many query processes to be created to run query to the streams, and one query_worker will get one query from query_list to run (i.e. on query_worker only keep running one query), query_list could be checked in the script. now the query_list is: 
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
