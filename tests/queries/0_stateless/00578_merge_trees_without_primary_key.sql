SELECT '*** MergeTree ***';

DROP STREAM IF EXISTS unsorted;
create stream unsorted (x uint32, y string) ENGINE MergeTree ORDER BY tuple() SETTINGS vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO unsorted VALUES (1, 'a'), (5, 'b');
INSERT INTO unsorted VALUES (2, 'c'), (4, 'd');
INSERT INTO unsorted VALUES (3, 'e');

OPTIMIZE STREAM unsorted PARTITION tuple() FINAL;

SELECT * FROM unsorted;

DROP STREAM unsorted;


SELECT '*** ReplacingMergeTree ***';

DROP STREAM IF EXISTS unsorted_replacing;

create stream unsorted_replacing (x uint32, s string, v uint32) ENGINE ReplacingMergeTree(v) ORDER BY tuple();

INSERT INTO unsorted_replacing VALUES (1, 'a', 5), (5, 'b', 4);
INSERT INTO unsorted_replacing VALUES (2, 'c', 3), (4, 'd', 2);
INSERT INTO unsorted_replacing VALUES (3, 'e', 1);

SELECT * FROM unsorted_replacing FINAL;

SELECT '---';

OPTIMIZE STREAM unsorted_replacing PARTITION tuple() FINAL;

SELECT * FROM unsorted_replacing;

DROP STREAM unsorted_replacing;


SELECT '*** CollapsingMergeTree ***';

DROP STREAM IF EXISTS unsorted_collapsing;

create stream unsorted_collapsing (x uint32, s string, sign int8) ENGINE CollapsingMergeTree(sign) ORDER BY tuple();

INSERT INTO unsorted_collapsing VALUES (1, 'a', 1);
INSERT INTO unsorted_collapsing VALUES (1, 'a', -1), (2, 'b', 1);
INSERT INTO unsorted_collapsing VALUES (2, 'b', -1), (3, 'c', 1);

SELECT * FROM unsorted_collapsing FINAL;

SELECT '---';

OPTIMIZE STREAM unsorted_collapsing PARTITION tuple() FINAL;

SELECT * FROM unsorted_collapsing;

DROP STREAM unsorted_collapsing;
