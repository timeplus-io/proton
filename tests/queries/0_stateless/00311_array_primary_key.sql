-- Tags: no-parallel

DROP STREAM IF EXISTS array_pk;
create stream array_pk (key array(uint8), s string, n uint64, d date MATERIALIZED '2000-01-01') ENGINE = MergeTree(d, (key, s, n), 1);

INSERT INTO array_pk VALUES ([1, 2, 3], 'Hello, world!', 1);
INSERT INTO array_pk VALUES ([1, 2], 'Hello', 2);
INSERT INTO array_pk VALUES ([2], 'Goodbye', 3);
INSERT INTO array_pk VALUES ([], 'abc', 4);
INSERT INTO array_pk VALUES ([2, 3, 4], 'def', 5);
INSERT INTO array_pk VALUES ([5, 6], 'ghi', 6);

SELECT * FROM array_pk ORDER BY n;

DETACH TABLE array_pk;
ATTACH TABLE array_pk;

SELECT * FROM array_pk ORDER BY n;

DROP STREAM array_pk;
