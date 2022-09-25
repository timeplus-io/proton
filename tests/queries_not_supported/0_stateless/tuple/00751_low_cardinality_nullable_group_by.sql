SET query_mode = 'table';
set allow_suspicious_low_cardinality_types = 1;
drop stream if exists low_null_float;
create stream low_null_float (a low_cardinality(Nullable(float64))) ENGINE = MergeTree order by tuple();
INSERT INTO low_null_float (a) SELECT if(number % 3 == 0, Null, number)  FROM system.numbers LIMIT 1000000;

SELECT a, count() FROM low_null_float GROUP BY a ORDER BY count() desc, a LIMIT 10;
drop stream if exists low_null_float;

