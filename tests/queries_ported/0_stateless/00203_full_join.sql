SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;

SELECT k, x, y FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, x FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, y FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT x, y FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;

SELECT k, x, y FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, x FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, y FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT x, y FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k FROM (SELECT array_join([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, array_join([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;

SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS t1_00203;
DROP STREAM IF EXISTS t2_00203;

create stream t1_00203 (k1 uint32, k2 uint32, k3 uint32, val_t1 string) ;
create stream t2_00203 (val_t2 string, k3 uint32, k2_alias uint32, k1 uint32) ;

INSERT INTO t1_00203(k1, k2, k3, val_t1) VALUES (1, 2, 3, 'aaa'), (2, 3, 4, 'bbb');
INSERT INTO t2_00203(val_t2, k3, k2_alias,k1) VALUES ('ccc', 4, 3, 2), ('ddd', 7, 6, 5);

SELECT sleep(3);

SELECT k1, k2, k3, val_t1, val_t2 FROM t1_00203 ANY FULL JOIN t2_00203 USING (k3, k1, k2 AS k2_alias) ORDER BY k1, k2, k3;
SELECT k1, k2, k3, val_t1, val_t2 FROM t1_00203 ANY RIGHT JOIN t2_00203 USING (k3, k1, k2 AS k2_alias) ORDER BY k1, k2, k3;

SET any_join_distinct_right_table_keys = 0;
SELECT k1, k2, k3, val_t1, val_t2 FROM t1_00203 ANY FULL JOIN t2_00203 USING (k3, k1, k2 AS k2_alias) ORDER BY k1, k2, k3; -- { serverError 48 }
SELECT k1, k2, k3, val_t1, val_t2 FROM t1_00203 ANY RIGHT JOIN t2_00203 USING (k3, k1, k2 AS k2_alias) ORDER BY k1, k2, k3;

DROP STREAM t1_00203;
DROP STREAM t2_00203;
