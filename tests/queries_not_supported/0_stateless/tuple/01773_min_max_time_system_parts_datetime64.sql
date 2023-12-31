DROP STREAM IF EXISTS test;
create stream test (time DateTime64(3)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY to_start_of_interval(time, INTERVAL 2 YEAR);

INSERT INTO test VALUES ('2000-01-02 03:04:05.123'), ('2001-02-03 04:05:06.789');

SELECT min_time, max_time FROM system.parts WHERE table = 'test' AND database = currentDatabase();
SELECT min_time, max_time FROM system.parts_columns WHERE table = 'test' AND database = currentDatabase();

DROP STREAM test;
