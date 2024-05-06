-- 01_create-tables.sql

CREATE STREAM IF NOT EXISTS nginx_historical_access_log
(
	remote_ip ipv4, 
	rfc1413_ident string,
	remote_user string,
	date_time_string string,
	date_time datetime64 default now64(),
	http_verb string,
	path string,
	http_ver string,
	status uint32,
	size uint32,
	referer string,
	user_agent string,
	malicious_request string
) 
ENGINE = MergeTree
ORDER BY remote_ip
SETTINGS event_time_column='date_time';

CREATE STREAM IF NOT EXISTS nginx_ipinfo 
(    
    remote_ip ipv4,
    city string,
    region string,
    country string,
    country_name string,
    country_flag_emoji string,
    country_flag_unicode string,
    continent_name string,
    isEU bool
)
ENGINE MergeTree
PRIMARY KEY remote_ip
ORDER BY remote_ip;
