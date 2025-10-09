DROP STREAM IF EXISTS t_missed_subcolumns;

-- .null subcolumn

CREATE STREAM t_missed_subcolumns (x uint32) ENGINE = MergeTree ORDER BY tuple_cast();
INSERT INTO t_missed_subcolumns SELECT * FROM numbers(10);

ALTER STREAM t_missed_subcolumns ADD COLUMN `y` nullable(uint32);

INSERT INTO t_missed_subcolumns SELECT number, if(number % 2, NULL, number) FROM numbers(10);

SELECT x FROM t_missed_subcolumns WHERE y IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT x FROM t_missed_subcolumns WHERE y IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;

DROP STREAM IF EXISTS t_missed_subcolumns;

-- .null and .size0 subcolumn in array

CREATE STREAM t_missed_subcolumns (id uint64, `n.a` array(nullable(string))) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_missed_subcolumns VALUES (1, ['aaa', NULL]) (2, ['ccc']) (3, [NULL]);
ALTER STREAM t_missed_subcolumns ADD COLUMN `n.b` array(nullable(string));
INSERT INTO t_missed_subcolumns VALUES (4, [NULL, 'bbb'], ['ddd', NULL]), (5, [NULL], [NULL]);

SELECT id, n.a, n.b FROM t_missed_subcolumns ORDER BY id;
SELECT id, n.a.size0, n.b.size0 FROM t_missed_subcolumns ORDER BY id;
SELECT id, n.a.null, n.b.null FROM t_missed_subcolumns ORDER BY id;

DROP STREAM IF EXISTS t_missed_subcolumns;

-- subcolumns and custom defaults

CREATE STREAM t_missed_subcolumns (id uint64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_missed_subcolumns;

INSERT INTO t_missed_subcolumns VALUES (1);

ALTER STREAM t_missed_subcolumns ADD COLUMN t tuple(a string, b string) DEFAULT ('foo', 'bar');
INSERT INTO t_missed_subcolumns VALUES (2, ('aaa', 'bbb'));

ALTER STREAM t_missed_subcolumns ADD COLUMN arr array(nullable(uint64)) DEFAULT [1, NULL, 3];
INSERT INTO t_missed_subcolumns VALUES (3, ('ccc', 'ddd'), [4, 5, 6]);

SELECT id, t, arr FROM t_missed_subcolumns ORDER BY id;
SELECT id, t.a, t.b, arr.size0, arr.null FROM t_missed_subcolumns ORDER BY id;

DROP STREAM t_missed_subcolumns;
