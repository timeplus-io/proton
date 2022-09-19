-- Tags: replica

DROP STREAM IF EXISTS t;
create stream t (x string) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES ('Hello');

SET max_parallel_replicas = 3;
SELECT * FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

DROP STREAM t;

create stream t (x string) ENGINE = MergeTree ORDER BY cityHash64(x) SAMPLE BY cityHash64(x);
INSERT INTO t SELECT to_string(number) FROM numbers(1000);

SET max_parallel_replicas = 1;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

SET max_parallel_replicas = 2;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

SET max_parallel_replicas = 3;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

DROP STREAM t;
