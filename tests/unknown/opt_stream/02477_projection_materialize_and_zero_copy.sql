DROP STREAM IF EXISTS t;

create stream t (c1 int64, c2 string, c3 DateTime, c4 int8, c5 string, c6 string, c7 string, c8 string, c9 string, c10 string, c11 string, c12 string, c13 int8, c14 int64, c15 string, c16 string, c17 string, c18 int64, c19 int64, c20 int64) engine ReplicatedMergeTree('/clickhouse/test/{database}/test_02477', '1') order by c18
SETTINGS allow_remote_fs_zero_copy_replication=1;

insert into t (c1, c18) select number, -number from numbers(2000000);

alter stream t add projection p_norm (select * order by c1);

optimize stream t final;

alter stream t materialize projection p_norm settings mutations_sync = 1;

SYSTEM FLUSH LOGS;

SELECT * FROM system.text_log WHERE event_time >= now() - 30 and level == 'Error' and message like '%BAD_DATA_PART_NAME%'and message like '%p_norm%';

DROP STREAM IF EXISTS t;
