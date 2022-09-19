-- Tags: shard

DROP STREAM IF EXISTS data;
create stream data (s string, x int8, y int8) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO data VALUES ('hello', 0, 0), ('world', 0, 0), ('hello', 1, -1), ('world', -1, 1);

SELECT DISTINCT s FROM remote('127.0.0.{1,2}', currentDatabase(), data) ORDER BY x + y, s;

DROP STREAM data;
