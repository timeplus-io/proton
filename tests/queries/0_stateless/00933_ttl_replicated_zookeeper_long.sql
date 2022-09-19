-- Tags: long, replica

DROP STREAM IF EXISTS ttl_repl1;
DROP STREAM IF EXISTS ttl_repl2;

create stream ttl_repl1(d date, x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00933/ttl_repl', '1')
    PARTITION BY to_day_of_month(d) ORDER BY x TTL d + INTERVAL 1 DAY;
create stream ttl_repl2(d date, x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00933/ttl_repl', '2')
    PARTITION BY to_day_of_month(d) ORDER BY x TTL d + INTERVAL 1 DAY;

INSERT INTO TABLE ttl_repl1 VALUES (to_date('2000-10-10 00:00:00'), 100);
INSERT INTO TABLE ttl_repl1 VALUES (to_date('2100-10-10 00:00:00'), 200);

ALTER STREAM ttl_repl1 MODIFY TTL d + INTERVAL 1 DAY;
SYSTEM SYNC REPLICA ttl_repl2;

INSERT INTO TABLE ttl_repl1 VALUES (to_date('2000-10-10 00:00:00'), 300);
INSERT INTO TABLE ttl_repl1 VALUES (to_date('2100-10-10 00:00:00'), 400);

SYSTEM SYNC REPLICA ttl_repl2;

SELECT sleep(1) format Null; -- wait for probable merges after inserts

OPTIMIZE STREAM ttl_repl2 FINAL;
SELECT x FROM ttl_repl2 ORDER BY x;

SHOW create stream ttl_repl2;

DROP STREAM ttl_repl1;
DROP STREAM ttl_repl2;
