 

DROP STREAM IF EXISTS mergetree_00698;
create stream mergetree_00698 (k uint32, `n.x` array(uint64), `n.y` array(uint64)) ENGINE = MergeTree ORDER BY k;

INSERT INTO mergetree_00698 VALUES (3, [], [1, 2, 3]), (1, [111], []), (2, [], []); -- { serverError 190 }
SELECT * FROM mergetree_00698;

INSERT INTO mergetree_00698 VALUES (3, [4, 5, 6], [1, 2, 3]), (1, [111], [222]), (2, [], []);
SELECT * FROM mergetree_00698;

DROP STREAM mergetree_00698;
