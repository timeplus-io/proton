SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS test_00615;

create stream test_00615
(
    dt date,
    id int32,
    key string,
    data nullable(int8)
) ENGINE = MergeTree(dt, (id, key, dt), 8192);

INSERT INTO test_00615 (dt,id, key,data) VALUES ('2000-01-01', 100, 'key', 100500);
SELECT sleep(3);


alter stream test_00615 drop column data;
alter stream test_00615 add column data nullable(float64);

INSERT INTO test_00615 (dt,id, key,data) VALUES ('2000-01-01', 100, 'key', 100500);
SELECT sleep(3);


SELECT * FROM test_00615 ORDER BY data NULLS FIRST;
OPTIMIZE TABLE test_00615;
SELECT * FROM test_00615 ORDER BY data NULLS FIRST;

DROP STREAM test_00615;
