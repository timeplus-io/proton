SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS prewhere_defaults;

create stream prewhere_defaults (d date DEFAULT '2000-01-01', k uint64 DEFAULT 0, x uint16) ENGINE = MergeTree(d, k, 1);

INSERT INTO prewhere_defaults (x) VALUES (1);
SELECT sleep(3);

SET max_block_size = 1;

SELECT * FROM prewhere_defaults PREWHERE x != 0 ORDER BY x;

ALTER STREAM prewhere_defaults ADD COLUMN y uint16 DEFAULT x;

SELECT * FROM prewhere_defaults PREWHERE x != 0 ORDER BY x;

INSERT INTO prewhere_defaults (x) VALUES (2);
SELECT sleep(3);

SELECT * FROM prewhere_defaults PREWHERE x != 0 ORDER BY x;

DROP STREAM prewhere_defaults;
