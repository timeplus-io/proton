DROP STREAM IF EXISTS test;

create stream test(number uint64, num2 uint64)  ;

INSERT INTO test WITH number * 2 AS num2 SELECT number, num2 FROM system.numbers LIMIT 3;

SELECT * FROM test;

DROP STREAM test;
