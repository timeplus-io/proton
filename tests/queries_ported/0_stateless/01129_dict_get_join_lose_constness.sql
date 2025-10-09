-- Tags: no-parallel

DROP DICTIONARY IF EXISTS system.dict1;

CREATE DICTIONARY IF NOT EXISTS system.dict1
(
    bytes_allocated uint64,
    element_count int32,
    loading_start_time datetime
)
PRIMARY KEY bytes_allocated
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'default' PASSWORD '' STREAM 'dictionaries' DB 'system'))
LIFETIME(0)
LAYOUT(hashed());

SELECT join_key,
       to_timezone(dict_get_datetime('system.dict1', 'loading_start_time', to_uint64(dict_key)), 'UTC') AS datetime
FROM (select dict_get_int32('system.dict1', 'element_count', to_uint64(dict_key)) AS join_key, 1 AS dict_key) as js1
LEFT JOIN (SELECT to_int32(2) AS join_key) as js2
USING (join_key)
WHERE now() >= datetime;

DROP DICTIONARY IF EXISTS system.dict1;