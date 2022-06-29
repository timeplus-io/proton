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


WS error when cancel:
select extract(raw, 'Code: ^[0-9]*$'), raw from proton_server_log where multi_search_any(raw, ['Code: 26']) settings seek_to='earliest'