DROP STREAM IF EXISTS t;

create stream t(`id` string, `dealer_id` string) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192;
insert into t(id, dealer_id) values('1','2');
SELECT * FROM t;
SET mutations_sync = 1;
ALTER STREAM t DELETE WHERE id in (select id from t as tmp);
SELECT '---';
SELECT * FROM t;

DROP STREAM t;
