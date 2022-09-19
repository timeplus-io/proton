SELECT 'array';

SELECT to_type_name([to_date('2000-01-01'), to_datetime('2000-01-01', 'Europe/Moscow')]);
SELECT to_type_name([to_date('2000-01-01'), to_datetime('2000-01-01', 'Europe/Moscow'), toDateTime64('2000-01-01', 5, 'Europe/Moscow')]);
SELECT to_type_name([to_date('2000-01-01'), to_datetime('2000-01-01', 'Europe/Moscow'), toDateTime64('2000-01-01', 5, 'Europe/Moscow'), toDateTime64('2000-01-01', 6, 'Europe/Moscow')]);

DROP STREAM IF EXISTS predicate_table;
create stream predicate_table (value uint8) ;

INSERT INTO predicate_table VALUES (0), (1);

SELECT 'If';

WITH to_date('2000-01-01') as a, to_datetime('2000-01-01', 'Europe/Moscow') as b
SELECT if(value, b, a) as result, to_type_name(result)
FROM predicate_table;

WITH to_datetime('2000-01-01', 'Europe/Moscow') as a, toDateTime64('2000-01-01', 5, 'Europe/Moscow') as b
SELECT if(value, b, a) as result, to_type_name(result)
FROM predicate_table;

SELECT 'Cast';
SELECT CAST(to_date('2000-01-01') AS datetime('UTC')) AS x, to_type_name(x);
SELECT CAST(to_date('2000-01-01') AS DateTime64(5, 'UTC')) AS x, to_type_name(x);
