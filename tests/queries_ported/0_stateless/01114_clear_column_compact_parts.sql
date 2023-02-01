DROP STREAM IF EXISTS clear_column;

CREATE STREAM clear_column(x uint32, y uint32) ENGINE MergeTree ORDER BY x PARTITION by x;
INSERT INTO clear_column VALUES (1, 1), (2, 3);

ALTER STREAM clear_column CLEAR COLUMN y IN PARTITION 1;
SELECT * FROM clear_column ORDER BY x;
ALTER STREAM clear_column CLEAR COLUMN y IN PARTITION 2;
SELECT * FROM clear_column ORDER BY x;

DROP STREAM clear_column;
