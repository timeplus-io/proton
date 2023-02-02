-- Tags: no-parallel

DROP STREAM IF EXISTS t;

SET mutations_sync = 1;
CREATE STREAM t (x uint8, s string) ENGINE = MergeTree ORDER BY x SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 15;

INSERT INTO t VALUES (1, 'hello');
SELECT * FROM t;

ALTER STREAM t UPDATE s = 'world' WHERE x = 1;
SELECT * FROM t;

DROP STREAM t;
