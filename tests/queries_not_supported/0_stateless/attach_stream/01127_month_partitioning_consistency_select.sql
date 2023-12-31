-- Tags: no-parallel

DROP STREAM IF EXISTS mt;
create stream mt (d date, x string) ENGINE = MergeTree(d, x, 8192);
INSERT INTO mt VALUES ('2106-02-07', 'Hello'), ('1970-01-01', 'World');

SELECT 'Q1', * FROM mt WHERE d = '2106-02-07';
SELECT 'Q2', * FROM mt WHERE d = '1970-01-01';

DETACH STREAM mt;
ATTACH STREAM mt;

SELECT 'Q1', * FROM mt WHERE d = '2106-02-07';
SELECT 'Q2', * FROM mt WHERE d = '1970-01-01';

DROP STREAM mt;
