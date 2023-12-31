-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS db_dict;
DROP DICTIONARY IF EXISTS db_dict.cache_hits;

CREATE DICTIONARY db_dict.cache_hits
(WatchID uint64, UserID uint64, SearchPhrase String)
PRIMARY KEY WatchID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hits' PASSWORD '' DB 'test'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(CACHE(SIZE_IN_CELLS 100 QUERY_WAIT_TIMEOUT_MILLISECONDS 600000));

SELECT sum(flag) FROM (SELECT dictHas('db_dict.cache_hits', to_uint64(WatchID)) as flag FROM table(test.hits) PREWHERE WatchID % 1400 == 0 LIMIT 100);
SELECT count() from table(test.hits) PREWHERE WatchID % 1400 == 0;

SELECT sum(flag) FROM (SELECT dictHas('db_dict.cache_hits', to_uint64(WatchID)) as flag FROM table(test.hits) PREWHERE WatchID % 350 == 0 LIMIT 100);
SELECT count() from table(test.hits) PREWHERE WatchID % 350 == 0;

SELECT sum(flag) FROM (SELECT dictHas('db_dict.cache_hits', to_uint64(WatchID)) as flag FROM table(test.hits) PREWHERE WatchID % 5 == 0 LIMIT 100);
SELECT count() from table(test.hits) PREWHERE WatchID % 5 == 0;

DROP DICTIONARY IF EXISTS db_dict.cache_hits;
DROP DATABASE IF  EXISTS db_dict;
