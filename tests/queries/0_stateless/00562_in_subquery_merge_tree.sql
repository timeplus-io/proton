DROP STREAM IF EXISTS merge_tree_in_subqueries;
create stream merge_tree_in_subqueries (id uint64, name string, num uint64) ENGINE = MergeTree ORDER BY (id, name);
INSERT INTO merge_tree_in_subqueries VALUES(1, 'test1', 42);
INSERT INTO merge_tree_in_subqueries VALUES(2, 'test2', 8);
INSERT INTO merge_tree_in_subqueries VALUES(3, 'test3', 8);
INSERT INTO merge_tree_in_subqueries VALUES(4, 'test4', 1985);
INSERT INTO merge_tree_in_subqueries VALUES(5, 'test5', 0);

-- Index scans.
SET force_primary_key = 1;

SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 0);

SELECT * FROM merge_tree_in_subqueries WHERE id IN (SELECT * FROM system.numbers LIMIT 2, 3) ORDER BY id;
SELECT * FROM merge_tree_in_subqueries WHERE name IN (SELECT 'test' || to_string(number) FROM system.numbers LIMIT 2, 3) ORDER BY id;

SELECT id AS id2, name AS value FROM merge_tree_in_subqueries WHERE (value, id2) IN (SELECT 'test' || to_string(number), number FROM system.numbers LIMIT 2, 3) ORDER BY id;

-- Non-index scans.
SET force_primary_key = 0;

SELECT id AS id2, name AS value FROM merge_tree_in_subqueries WHERE num IN (SELECT number FROM system.numbers LIMIT 10) ORDER BY id;
SELECT id AS id2, name AS value FROM merge_tree_in_subqueries WHERE (id, num) IN (SELECT number, number + 6 FROM system.numbers LIMIT 10) ORDER BY id;

DROP STREAM IF EXISTS merge_tree_in_subqueries;
