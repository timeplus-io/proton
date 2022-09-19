-- Tags: shard

DROP STREAM IF EXISTS remote_test;
create stream remote_test(uid string, its uint32,  action_code string, day date) ENGINE = MergeTree(day, (uid, its), 8192);
INSERT INTO remote_test SELECT to_string(number) AS uid, number % 3 AS its, to_string(number % 3) AS action_code, '2000-01-01' FROM system.numbers LIMIT 10000;
SELECT level, COUNT() FROM (SELECT uid, windowFunnel(3600)(to_uint32(its), action_code != '', action_code = '2') AS level FROM remote('127.0.0.{2,3}', currentDatabase(), remote_test) GROUP BY uid) GROUP BY level;
SELECT level, COUNT() FROM (SELECT uid, windowFunnel(3600)(to_uint32(its), action_code != '', action_code = '2') AS level FROM remote('127.0.0.{2,3}', currentDatabase(), remote_test) GROUP BY uid) GROUP BY level;
SELECT level, COUNT() FROM (SELECT uid, windowFunnel(3600)(to_uint32(its), action_code != '', action_code = '2') AS level FROM remote('127.0.0.{2,3}', currentDatabase(), remote_test) GROUP BY uid) GROUP BY level;
DROP STREAM IF EXISTS remote_test;
