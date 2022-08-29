# Neutron Errors:
SELECT count(*) FROM timeplus_long_log where multi_search_any(raw, ['error', 'Error', 'ERROR']) and multi_search_any (raw, ['neutron']) settings seek_to='earliest'

#Neutron Patics:
SELECT count() FROM timeplus_long_log where multi_search_any(raw, ['panic']) settings seek_to='earliest'

#Total Proton Logs Collected:
select count() from proton_server_log settings seek_to='earliest'

#Proton Exceptions:
select count() from proton_server_log where multi_search_any(raw, ['Exception', 'Error', 'Fatal']) settings seek_to='earliest'


select extract(log_type_prefix, '<.*[a-zA-Z]+>') as log_type, count() from (select extract(raw, '\[.*\].*[^}]*.*<.*>') as log_type_prefix from proton_server_log) group by log_type settings seek_to = 'earliest'

#proton fatal or memory limit hit:
select count() from proton_server_log where multi_search_any(raw, ['MEMORY_LIMIT_EXCEEDED', '<Fatal>']) settings seek_to='earliest'

#extract timestamp and query_time from proton_serve_log

select to_datetime(query_info[1][1]) as timestamp, to_float(query_info[2][1]) as query_time, extractAllGroupsHorizontal(message, '(\d{1,4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2})\.\d{1,} .* in (\d{1,}\.\d{1,})') as query_info, message from proton_server_log where multi_search_any(message, ['d579c41c-30a6-4867-a7fe-0c596e9a8cc7']) and not multi_search_any(message, ['<Error>']) settings seek_to = '-5h'

#extract error msg:
SELECT
  exception_info[1][1] as timeseamp, exception_info[2][1] as exception_source, exception_info[3][1] as error_code, exception_info[4][1] as exception_detail, extractAllGroupsHorizontal(message, '(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.[0-9]+) .*{.*} <\w+> (\w+): Code: ([0-9]+)\..*Exception: ([\w+\' ]+)') AS exception_info, message
 FROM
  proton_server_err
WHERE
  multi_search_any(message, ['Code:', '<Error>'])
settings seek_to='earliest'

#export csv:
docker exec -i proton-server proton-client --query="select to_datetime(query_info[1][1]) as timestamp, to_float(query_info[2][1]) as query_time, extractAllGroupsHorizontal(message, '(\d{1,4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2})\.\d{1,} .* in (\d{1,}\.\d{1,})') as query_info, message from proton_server_log where multi_search_any(message, ['d579c41c-30a6-4867-a7fe-0c596e9a8cc7']) and not multi_search_any(message, ['<Error>']) settings seek_to = '-5h' format CSV" > query_time.csv

SELECT
   query_info[2][1] as query_id, to_datetime((query_info[1])[1]) AS timestamp, to_float((query_info[3])[1]) AS query_time, extractAllGroupsHorizontal(message, '(\d{1,4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2})\.\d{1,} .* {(.*)} .* in (\d{1,}\.\d{1,})') AS query_info, message FROM
 default.proton_server_log WHERE
  multi_search_any(message, ['d579c41c-30a6-4867-a7fe-0c596e9a8cc7']) AND (NOT multi_search_any(message, ['<Error>'])) SETTINGS
  seek_to = '-5h'

#query_time of all queries and timestamp > a specific datetime
SELECT (query_info[2])[1] AS query_id, to_datetime64((query_info[1])[1],3) AS timestamp, to_float((query_info[3])[1]) AS query_time, extractAllGroupsHorizontal(message, '(\\d{1,4}\\.\\d{2}\\.\\d{2} \\d{2}:\\d{2}:\\d{2})\\.\\d{1,} .* {(.*)} .* in (\\d{1,}\\.\\d{1,})') AS query_info, message FROM default.proton_server_log WHERE multi_search_any(message, ['<Information> executeQuery']) AND (NOT multi_search_any(message, ['<Error>'])) and timestamp > to_datetime64('2022-06-22 16:00:00',3) settings seek_to='earliest'

SELECT
  (query_info[2])[1] AS query_id, to_datetime64((query_info[1])[1], 3) AS timestamp, to_float((query_info[3])[1]) AS query_time, extractAllGroupsHorizontal(message, '(\\d{1,4}\\.\\d{2}\\.\\d{2} \\d{2}:\\d{2}:\\d{2})\\.\\d{1,} .* {(.*)} .* in (\\d{1,}\\.\\d{1,})') AS query_info, message
FROM
 default.proton_server_log
WHERE
  multi_search_any(message, ['<Information> executeQuery']) AND (NOT multi_search_any(message, ['<Error>'])) AND (timestamp > to_datetime64('2022-06-29T00:00:00.000', 3))
SETTINGS
  seek_to = '2022-06-28T11:00:00.000'
