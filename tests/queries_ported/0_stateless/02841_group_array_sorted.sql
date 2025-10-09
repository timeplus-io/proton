SELECT group_array_sorted(number, 5) FROM numbers(100);

SELECT group_array_sorted(number, 10) FROM numbers(5);

SELECT group_array_sorted(number, 100) FROM numbers(1000);

SELECT group_array_sorted(str, 30) FROM (SELECT to_string(number) as str FROM numbers(30));

SELECT group_array_sorted(to_int64(number/2), 10) FROM numbers(100);

DROP STREAM IF EXISTS test;
CREATE STREAM test (a array(uint64)) engine=MergeTree ORDER BY a;
INSERT INTO test (a) VALUES ([3,4,5,6]), ([1,2,3,4]), ([2,3,4,5]);
SELECT SLEEP(3);
SELECT group_array_sorted(a, 3) FROM test;
DROP STREAM test;

CREATE STREAM IF NOT EXISTS test (id int32, data tuple(int32, int32)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, (100, 200)), (2, (15, 25)), (3, (2, 1)), (4, (30, 60));
SELECT SLEEP(3);
SELECT group_array_sorted(data, 4) FROM test;
DROP STREAM test;

CREATE STREAM IF NOT EXISTS test (id int32, data decimal32(2)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, 12.5), (2, 0.2), (3, 6.6), (4, 2.2);
SELECT SLEEP(3);
SELECT group_array_sorted(data, 4) FROM test;
DROP STREAM test;

CREATE STREAM IF NOT EXISTS test (id int32, data fixed_string(3)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, 'AAA'), (2, 'bbc'), (3, 'abc'), (4, 'aaa'), (5, 'Aaa');
SELECT SLEEP(3);
SELECT group_array_sorted(data, 5) FROM test;
DROP STREAM test;

CREATE STREAM test (id decimal(76, 53), str string) ENGINE = MergeTree ORDER BY id;
INSERT INTO test SELECT number, 'test' FROM numbers(1000000);
SELECT SLEEP(3);
SELECT count(id) FROM test;
SELECT count(concat(to_string(id), 'a')) FROM test;
DROP STREAM test;

CREATE STREAM test (id uint64, agg aggregate_function(group_array_sorted(2), uint64)) engine=MergeTree ORDER BY id;
INSERT INTO test SELECT 1, group_array_sorted_state(number, 2) FROM numbers(10);
SELECT SLEEP(3);
SELECT group_array_sorted_merge(agg, 2) FROM test;
DROP STREAM test;
