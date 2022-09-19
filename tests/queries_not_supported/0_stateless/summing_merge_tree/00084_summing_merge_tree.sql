DROP STREAM IF EXISTS summing_merge_tree;

create stream summing_merge_tree (d date, a string, x uint32, y uint64, z float64) ENGINE = SummingMergeTree(d, a, 8192);

INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Hello', 1, 2, 3);
INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Hello', 4, 5, 6);
INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Goodbye', 1, 2, 3);

OPTIMIZE STREAM summing_merge_tree;
OPTIMIZE STREAM summing_merge_tree;
OPTIMIZE STREAM summing_merge_tree;

SELECT * FROM summing_merge_tree ORDER BY d, a, x, y, z;


DROP STREAM summing_merge_tree;

create stream summing_merge_tree (d date, a string, x uint32, y uint64, z float64) ENGINE = SummingMergeTree(d, a, 8192, (y, z));

INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Hello', 1, 2, 3);
INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Hello', 4, 5, 6);
INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Goodbye', 1, 2, 3);

OPTIMIZE STREAM summing_merge_tree;
OPTIMIZE STREAM summing_merge_tree;
OPTIMIZE STREAM summing_merge_tree;

SELECT * FROM summing_merge_tree ORDER BY d, a, x, y, z;


DROP STREAM summing_merge_tree;

--
DROP STREAM IF EXISTS summing;
create stream summing (p date, k uint64, s uint64) ENGINE = SummingMergeTree(p, k, 1);

INSERT INTO summing (k, s) VALUES (0, 1);
INSERT INTO summing (k, s) VALUES (0, 1), (666, 1), (666, 0);
OPTIMIZE STREAM summing PARTITION 197001;

SELECT k, s FROM summing ORDER BY k;

DROP STREAM summing;
