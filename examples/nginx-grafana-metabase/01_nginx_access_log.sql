-- 01_nginx_access_log.sql

DROP STREAM IF EXISTS nginx_access_log;
CREATE RANDOM STREAM IF NOT EXISTS nginx_access_log (
  remote_ip ipv4 default random_in_type('ipv4'),
  rfc1413_ident string default '-',
  remote_user string default '-',
  date_time datetime64 default random_in_type('datetime64', 365, y -> to_time('2023-6-6') + interval y day),
  http_verb string default ['GET', 'POST', 'PUT', 'DELETE', 'HEAD'][rand()%5],
  path string default ['/rss/', '/', '/programmatic-creation-of-the-ghost-admin-user/', '/sitemap.xml', '/favicon.ico', '/Core/Skin/Login.aspx', '/Public/home/js/check.js', '/robots.txt', '/engineering-vs-technology-company/', '/switching-to-a-newer-version-of-rsync-on-macos/', '/.env'][rand()%11],
  http_ver string default ['HTTP/1.0', 'HTTP/1.1', 'HTTP/2.0'][rand()%3],
  status int default [200, 301, 302, 304, 400, 404][rand()%6],
  size int default rand()%5000000,
  referer string default ['-', 'https://ayewo.com/', 'https://ayewo.com/rss/', 'https://google.com/'][rand()%4],
  user_agent string default [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/' || to_string(rand()%30 + 100) || '.0.0.0 Safari/537.36',
    'FreshRSS/1.23.1 (Linux; https://freshrss.org)',
    'Tiny Tiny RSS/22.08-ed2cbef (Unsupported) (https://tt-rss.org/)',
    'NetNewsWire (RSS Reader; https://netnewswire.com/)',
    'Feedbin feed-id:2688391 - 12 subscribers',
    'Bifolia:0.1.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) News/Explorer/1.20',
    'GuzzleHttp/7',
    'Anthill',
    'Unread RSS Reader - https://www.goldenhillsoftware.com/unread/',
    'Blogtrottr/2.0',
    'Yarr/1.0',
    'Plenary/4.6.2',
    'Ruby'
  ][rand()%14],
  malicious_request string default if(rand()%100 < 5, '\x16\x03\x01\x00\xEA\x01\x00\x00\xE6\x03\x03', '')
);


DROP STREAM IF EXISTS nginx_historical_access_log;
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
ORDER BY remote_ip
SETTINGS event_time_column='date_time';


DROP STREAM IF EXISTS nginx_ipinfo;
CREATE STREAM IF NOT EXISTS nginx_ipinfo 
(    
    ip ipv4,
    city string,
    region string,
    country string,
    country_name string,
    country_flag_emoji string,
    country_flag_unicode string,
    continent_name string,
    isEU bool,
    loc string
)
PRIMARY KEY ip
ORDER BY ip;
