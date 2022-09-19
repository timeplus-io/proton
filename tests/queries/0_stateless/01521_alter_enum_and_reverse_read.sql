DROP STREAM IF EXISTS enum_test;

create stream enum_test(timestamp DateTime, host string, e Enum8('IU' = 1, 'WS' = 2)) Engine = MergeTree PARTITION BY to_date(timestamp) ORDER BY (timestamp, host);

INSERT INTO enum_test SELECT '2020-10-09 00:00:00', 'h1', 'WS' FROM numbers(1);

ALTER STREAM enum_test MODIFY COLUMN e Enum8('IU' = 1, 'WS' = 2, 'PS' = 3);

INSERT INTO enum_test SELECT '2020-10-09 00:00:00', 'h1', 'PS' from numbers(1);

SELECT * FROM enum_test ORDER BY timestamp DESC;

DROP STREAM IF EXISTS enum_test;
