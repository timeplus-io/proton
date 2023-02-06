SELECT 'array';

SELECT to_type_name([toDate('2000-01-01'), to_datetime('2000-01-01', 'Asia/Istanbul')]);
SELECT to_type_name([toDate('2000-01-01'), to_datetime('2000-01-01', 'Asia/Istanbul'), to_datetime64('2000-01-01', 5, 'Asia/Istanbul')]);
SELECT to_type_name([toDate('2000-01-01'), to_datetime('2000-01-01', 'Asia/Istanbul'), to_datetime64('2000-01-01', 5, 'Asia/Istanbul'), to_datetime64('2000-01-01', 6, 'Asia/Istanbul')]);

DROP STREAM IF EXISTS predicate_table;
CREATE STREAM predicate_table (value uint8) ENGINE=TinyLog;

INSERT INTO predicate_table VALUES (0), (1);

SELECT 'If';

WITH toDate('2000-01-01') as a, to_datetime('2000-01-01', 'Asia/Istanbul') as b
SELECT if(value, b, a) as result, to_type_name(result)
FROM predicate_table;

WITH to_datetime('2000-01-01', 'Asia/Istanbul') as a, to_datetime64('2000-01-01', 5, 'Asia/Istanbul') as b
SELECT if(value, b, a) as result, to_type_name(result)
FROM predicate_table;

SELECT 'Cast';
SELECT CAST(toDate('2000-01-01') AS DateTime('UTC')) AS x, to_type_name(x);
SELECT CAST(toDate('2000-01-01') AS DateTime64(5, 'UTC')) AS x, to_type_name(x);
