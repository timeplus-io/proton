-- Tags: not_supported, blocked_by_SummingMergeTree

DROP STREAM IF EXISTS pk_func;
create stream pk_func(d DateTime, ui uint32) ENGINE = SummingMergeTree ORDER BY to_date(d);

INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(100000);
INSERT INTO pk_func SELECT '2020-05-06 01:00:00', number FROM numbers(100000);
INSERT INTO pk_func SELECT '2020-05-07 01:00:00', number FROM numbers(100000);

SELECT to_date(d), ui FROM pk_func FINAL order by d;

DROP STREAM pk_func;
