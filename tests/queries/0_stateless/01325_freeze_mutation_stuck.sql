DROP STREAM IF EXISTS mt;
create stream mt (x string, y uint64, INDEX idx (y) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY y;
INSERT INTO mt VALUES ('Hello, world', 1);

SELECT * FROM mt;
ALTER STREAM mt FREEZE;
SELECT * FROM mt;

SET mutations_sync = 1;
ALTER STREAM mt UPDATE x = 'Goodbye' WHERE y = 1;
SELECT * FROM mt;

DROP STREAM mt;
