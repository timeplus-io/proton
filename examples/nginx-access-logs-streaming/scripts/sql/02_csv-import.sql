-- 02_csv-import.sql

INSERT INTO nginx_historical_access_log (remote_ip, rfc1413_ident, remote_user, date_time_string, http_verb, path, http_ver, status, size, referer, user_agent, malicious_request) 
SELECT remote_ip, rfc1413_ident, remote_user, date_time, http_verb, path, http_ver, status, size, referer, user_agent, malicious_request 
FROM file('access.log.csv', 'CSVWithNames', 'remote_ip ipv4, rfc1413_ident string, remote_user string, date_time string, http_verb string, path string, http_ver string, status uint32, size uint32, referer string, user_agent string, malicious_request string')
SETTINGS max_insert_threads=8;




set input_format_skip_unknown_fields = 1;

INSERT INTO nginx_ipinfo (remote_ip, city, region, country, country_name, country_flag_emoji, country_flag_unicode, continent_name, isEU) 
SELECT ip, city, region, country, country_name, country_flag_emoji, country_flag_unicode, continent_name, isEU 
FROM file('access.ipinfo.csv', 'CSVWithNames', 'ip ipv4, city string, region string, country string, country_name string, country_flag_emoji string, country_flag_unicode string, continent_name string, isEU bool')
SETTINGS max_insert_threads=8;
