DROP STREAM IF EXISTS userid_test;

SET use_index_for_in_with_subqueries = 1;

CREATE STREAM userid_test (userid uint64) ENGINE = MergeTree() PARTITION BY (int_div(userid, 500)) ORDER BY (userid) SETTINGS index_granularity = 8192;

INSERT INTO userid_test VALUES (1),(2),(3),(4),(5);

DROP STREAM IF EXISTS userid_set;

CREATE STREAM userid_set(userid uint64) ENGINE = Set;

INSERT INTO userid_set VALUES (1),(2),(3);

SELECT * FROM userid_test WHERE userid IN (1, 2, 3);

SELECT * FROM userid_test WHERE to_uint64(1) IN (userid_set);

SELECT * FROM userid_test WHERE userid IN (userid_set);

DROP STREAM userid_test;
DROP STREAM userid_set;
