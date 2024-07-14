-- 02_csv-export.sql

SELECT replace(ipv4_num_to_string_class_c(remote_ip),'xxx','0') as rip, 
remote_ip, rfc1413_ident, remote_user, 
date_time, http_verb, path, http_ver, 
status, size, referer, user_agent, malicious_request 
FROM nginx_access_log LIMIT 20 FORMAT CSV;