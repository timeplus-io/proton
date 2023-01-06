SET query_mode='table';

DROP STREAM IF EXISTS test_00688;

create stream test_00688 (a uint8) ;

INSERT INTO test_00688(a) VALUES (1), (2), (1), (3);

SELECT sleep(3);

SELECT CASE WHEN a=1 THEN 0 END FROM test_00688;

DROP STREAM test_00688;
