SET query_mode='table';
DROP STREAM IF EXISTS test_00681;

create stream test_00681(x int32)  ;
INSERT INTO test_00681(x) VALUES (123);

select sleep(3);

SELECT a1 
FROM
(
    SELECT x AS a1, x AS a2 FROM test_00681
    UNION ALL
    SELECT x, x FROM test_00681
);

DROP STREAM test_00681;
