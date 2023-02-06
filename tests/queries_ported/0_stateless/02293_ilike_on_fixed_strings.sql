DROP STREAM IF EXISTS tab;

CREATE STREAM tab (col fixed_string(2)) engine = MergeTree() ORDER BY col;

INSERT INTO tab VALUES ('AA') ('Aa');

SELECT col, col LIKE '%a', col ILIKE '%a' FROM tab WHERE col = 'AA';
SELECT col, col LIKE '%a', col ILIKE '%a' FROM tab WHERE col = 'Aa';

DROP STREAM IF EXISTS tab;
