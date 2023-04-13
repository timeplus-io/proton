DROP STREAM IF EXISTS join_tbl;

CREATE STREAM join_tbl (`id` string, `name` string, lcname low_cardinality(string)) ENGINE = Join(any, left, id);

INSERT INTO join_tbl VALUES ('xxx', 'yyy', 'yyy');

SELECT join_get('join_tbl', 'name', 'xxx') == 'yyy';
SELECT join_get('join_tbl', 'name', to_low_cardinality('xxx')) == 'yyy';
SELECT join_get('join_tbl', 'name', to_low_cardinality(materialize('xxx'))) == 'yyy';
SELECT join_get('join_tbl', 'lcname', 'xxx') == 'yyy';
SELECT join_get('join_tbl', 'lcname', to_low_cardinality('xxx')) == 'yyy';
SELECT join_get('join_tbl', 'lcname', to_low_cardinality(materialize('xxx'))) == 'yyy';

DROP STREAM IF EXISTS join_tbl;
