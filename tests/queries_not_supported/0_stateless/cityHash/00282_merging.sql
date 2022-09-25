SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS merge;
create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) VALUES (1), (2), (3);
INSERT INTO merge (x) VALUES (4), (5), (6);

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP STREAM merge;


create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number      AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 10 AS x FROM system.numbers LIMIT 10;

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP STREAM merge;


create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number + 5 AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number     AS x FROM system.numbers LIMIT 10;

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP STREAM merge;


create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number + 5 AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number     AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 9 AS x FROM system.numbers LIMIT 10;

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP STREAM merge;


create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number      AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 5  AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 10 AS x FROM system.numbers LIMIT 10;

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

INSERT INTO merge (x) SELECT number + 5  AS x FROM system.numbers LIMIT 10;

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

INSERT INTO merge (x) SELECT number + 100  AS x FROM system.numbers LIMIT 10;

SELECT sleep(3);


SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE STREAM merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP STREAM merge;


create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 8192);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 8200;
INSERT INTO merge (x) SELECT number AS x FROM (SELECT * FROM system.numbers LIMIT 8200) ORDER BY rand();
INSERT INTO merge (x) SELECT number AS x FROM (SELECT * FROM system.numbers LIMIT 8200) ORDER BY rand();

SELECT sleep(3);


OPTIMIZE STREAM merge;

SELECT count(), uniqExact(x), min(x), max(x), sum(x), sum(cityHash64(x)) FROM merge;

DROP STREAM merge;


create stream IF NOT EXISTS merge (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 8192);

SET max_block_size = 10000;
INSERT INTO merge (x) SELECT number AS x FROM (SELECT number FROM system.numbers LIMIT 10000);
INSERT INTO merge (x) SELECT number AS x FROM (SELECT number + 5000 AS number FROM system.numbers LIMIT 10000);

SELECT sleep(3);


OPTIMIZE STREAM merge;

SELECT count(), uniqExact(x), min(x), max(x), sum(x), sum(cityHash64(x)) FROM merge;

DROP STREAM merge;
