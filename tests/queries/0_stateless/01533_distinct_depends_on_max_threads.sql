DROP STREAM IF EXISTS bug_13492;

create stream bug_13492 (`d` DateTime) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(d) ORDER BY tuple();

INSERT INTO bug_13492 SELECT addDays(now(), number) FROM numbers(100);

SET max_threads = 5;

SELECT DISTINCT 1 FROM bug_13492, numbers(1) n;

SET max_threads = 2;

SELECT DISTINCT 1 FROM bug_13492, numbers(1) n;

DROP STREAM bug_13492;
