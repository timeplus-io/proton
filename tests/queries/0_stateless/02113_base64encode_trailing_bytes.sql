-- Tags: no-fasttest
SET log_queries=1;

DROP STREAM IF EXISTS tabl_1;
DROP STREAM IF EXISTS tabl_2;

create stream tabl_1 (key string) ENGINE MergeTree ORDER BY key;
create stream tabl_2 (key string) ENGINE MergeTree ORDER BY key;
SELECT * FROM tabl_1 SETTINGS log_comment = 'ad15a651';
SELECT * FROM tabl_2 SETTINGS log_comment = 'ad15a651';
SYSTEM FLUSH LOGS;

SELECT base64Decode(base64Encode(normalizeQuery(query)))
    FROM system.query_log
    WHERE type = 'QueryFinish' AND log_comment = 'ad15a651' AND current_database = currentDatabase()
    GROUP BY normalizeQuery(query)
    ORDER BY normalizeQuery(query);

DROP STREAM tabl_1;
DROP STREAM tabl_2;
