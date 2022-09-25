DROP STREAM IF EXISTS test_insert_t1;
DROP STREAM IF EXISTS test_insert_t2;
DROP STREAM IF EXISTS test_insert_t3;

create stream test_insert_t1 (`dt` date, `uid` string, `name` string, `city` string) ENGINE = MergeTree PARTITION BY to_YYYYMMDD(dt) ORDER BY name SETTINGS index_granularity = 8192;
create stream test_insert_t2 (`dt` date, `uid` string) ENGINE = MergeTree PARTITION BY to_YYYYMMDD(dt) ORDER BY uid SETTINGS index_granularity = 8192;
create stream test_insert_t3 (`dt` date, `uid` string, `name` string, `city` string) ENGINE = MergeTree PARTITION BY to_YYYYMMDD(dt) ORDER BY name SETTINGS index_granularity = 8192;

INSERT INTO test_insert_t1 SELECT '2019-09-01',to_string(number),to_string(rand()),to_string(rand()) FROM system.numbers WHERE number > 10 limit 1000000;
INSERT INTO test_insert_t2 SELECT '2019-09-01',to_string(number) FROM system.numbers WHERE number >=0 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',to_string(number) FROM system.numbers WHERE number >=100000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',to_string(number) FROM system.numbers WHERE number >=300000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',to_string(number) FROM system.numbers WHERE number >=500000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',to_string(number) FROM system.numbers WHERE number >=700000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',to_string(number) FROM system.numbers WHERE number >=900000 limit 200;

INSERT INTO test_insert_t3 SELECT '2019-09-01', uid, name, city FROM ( SELECT dt, uid, name, city FROM test_insert_t1 WHERE dt = '2019-09-01') t1 GLOBAL SEMI LEFT JOIN (SELECT uid FROM test_insert_t2 WHERE dt = '2019-09-01') t2 ON t1.uid=t2.uid;

SELECT count(*) FROM test_insert_t3;

DROP STREAM test_insert_t1;
DROP STREAM test_insert_t2;
DROP STREAM test_insert_t3;
