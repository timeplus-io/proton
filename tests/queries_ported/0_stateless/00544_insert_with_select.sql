SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS test;

create stream test(number uint64, num2 uint64)  ;

INSERT INTO test(number, num2) WITH number * 2 AS num2 SELECT number, num2 FROM system.numbers LIMIT 3;
SELECT sleep(3);

SELECT * FROM test;

DROP STREAM test;
