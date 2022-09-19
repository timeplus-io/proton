-- Tags: not_supported, blocked_by_SummingMergeTree

DROP STREAM IF EXISTS m3;
DROP STREAM IF EXISTS m1;
DROP STREAM IF EXISTS x;

create stream x (d date, t DateTime) ENGINE = MergeTree(d, (d, t), 1);

CREATE MATERIALIZED VIEW m1 (d date, t DateTime, c uint64) ENGINE = SummingMergeTree(d, (d, t), 1) AS SELECT d, toStartOfMinute(x.t) as t, count() as c FROM x GROUP BY d, t;

CREATE MATERIALIZED VIEW m3 ENGINE = SummingMergeTree(d, (d, t), 1) AS SELECT d, toStartOfHour(m1.t) as t, c FROM m1;

INSERT INTO x VALUES (today(), now());
INSERT INTO x VALUES (today(), now());

OPTIMIZE STREAM m3;

DROP STREAM m3;
DROP STREAM m1;
DROP STREAM x;
