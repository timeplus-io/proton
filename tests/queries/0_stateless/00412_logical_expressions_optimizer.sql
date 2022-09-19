DROP STREAM IF EXISTS merge_tree;
create stream merge_tree (x uint64, date date) ENGINE = MergeTree(date, x, 1);

INSERT INTO merge_tree VALUES (1, '2000-01-01');
SELECT x AS y, y FROM merge_tree;

DROP STREAM IF EXISTS merge_tree;
