DROP STREAM IF EXISTS foo;

create stream foo (ts DateTime, x uint64)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts)
ORDER BY (ts);

INSERT INTO foo (ts, x) SELECT to_datetime('2020-01-01 00:05:00'), number from system.numbers_mt LIMIT 10;

SET mutations_sync = 1;

ALTER STREAM foo UPDATE x = 1 WHERE x = (SELECT x from foo WHERE x = 4);

SELECT sum(x) == 42 FROM foo;

ALTER STREAM foo UPDATE x = 1 WHERE x IN (SELECT x FROM foo WHERE x != 0);

SELECT sum(x) == 9 FROM foo;

DROP STREAM IF EXISTS bar;

create stream bar (ts DateTime, x uint64)
;

INSERT INTO bar (ts, x) SELECT to_datetime('2020-01-01 00:05:00'), number from system.numbers_mt LIMIT 10;

SET mutations_sync = 1;

ALTER STREAM bar UPDATE x = 1 WHERE x = (SELECT x from bar WHERE x = 4);

SELECT sum(x) == 42 FROM bar;

ALTER STREAM bar UPDATE x = 1 WHERE x IN (SELECT x FROM bar WHERE x != 0);

SELECT sum(x) == 9 FROM bar;
