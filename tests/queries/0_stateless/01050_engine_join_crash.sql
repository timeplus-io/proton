DROP STREAM IF EXISTS testJoinTable;

SET any_join_distinct_right_table_keys = 1;
SET enable_optimize_predicate_expression = 0;

create stream testJoinTable (number uint64, data string) ENGINE = Join(ANY, INNER, number) SETTINGS any_join_distinct_right_table_keys = 1;

INSERT INTO testJoinTable VALUES (1, '1'), (2, '2'), (3, '3');

SELECT * FROM (SELECT * FROM numbers(10)) js1 INNER JOIN testJoinTable USING number; -- { serverError 264 }
SELECT * FROM (SELECT * FROM numbers(10)) js1 INNER JOIN (SELECT * FROM testJoinTable) js2 USING number;
SELECT * FROM (SELECT * FROM numbers(10)) js1 ANY INNER JOIN testJoinTable USING number;
SELECT * FROM testJoinTable;

DROP STREAM testJoinTable;

SELECT '-';
 
DROP STREAM IF EXISTS master;
DROP STREAM IF EXISTS transaction;

create stream transaction (id int32, value float64, master_id int32) ENGINE = MergeTree() ORDER BY id;
create stream master (id int32, name string) ENGINE = Join (ANY, LEFT, id) SETTINGS any_join_distinct_right_table_keys = 1;

INSERT INTO master VALUES (1, 'ONE');
INSERT INTO transaction VALUES (1, 52.5, 1);

SELECT tx.id, tx.value, m.name FROM transaction tx ANY LEFT JOIN master m ON m.id = tx.master_id;

DROP STREAM master;
DROP STREAM transaction;

SELECT '-';

DROP STREAM IF EXISTS some_join;
DROP STREAM IF EXISTS tbl;

create stream tbl (eventDate date, id string) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY eventDate;
create stream some_join (id string, value string) ENGINE = Join(ANY, LEFT, id) SETTINGS any_join_distinct_right_table_keys = 1;

SELECT * FROM tbl AS t ANY LEFT JOIN some_join USING (id);
SELECT * FROM tbl AS t ANY LEFT JOIN some_join AS d USING (id);
-- TODO SELECT t.*, d.* FROM tbl AS t ANY LEFT JOIN some_join AS d USING (id);

DROP STREAM some_join;
DROP STREAM tbl;
