-- Tags: no-tsan, no-parallel

CREATE DATABASE IF NOT EXISTS db_dict;
DROP DICTIONARY IF EXISTS db_dict.cache_hits;

CREATE DICTIONARY db_dict.cache_hits
(WatchID uint64, UserID uint64, SearchPhrase String)
PRIMARY KEY WatchID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hits' PASSWORD '' DB 'test'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(CACHE(SIZE_IN_CELLS 1 QUERY_WAIT_TIMEOUT_MILLISECONDS 60000));

SELECT count() FROM (SELECT WatchID,  array_distinct(group_array(dictGetuint64( 'db_dict.cache_hits', 'UserID', to_uint64(WatchID)))) as arr
FROM table(test.hits) PREWHERE WatchID % 5 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

SELECT count() FROM (SELECT WatchID,  array_distinct(group_array(dictGetuint64( 'db_dict.cache_hits', 'UserID', to_uint64(WatchID)))) as arr
FROM table(test.hits) PREWHERE WatchID % 7 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

SELECT count() FROM (SELECT WatchID,  array_distinct(group_array(dictGetuint64( 'db_dict.cache_hits', 'UserID', to_uint64(WatchID)))) as arr
FROM table(test.hits) PREWHERE WatchID % 13 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

DROP DICTIONARY IF EXISTS db_dict.cache_hits;
DROP DATABASE IF  EXISTS db_dict;
