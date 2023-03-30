SET any_join_distinct_right_table_keys = 1;
SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS t1_00848;
DROP STREAM IF EXISTS t2_00848;
DROP STREAM IF EXISTS t3_00848;
CREATE STREAM t1_00848 ( id string ) ENGINE = Memory;
CREATE STREAM t2_00848 ( id nullable(string) ) ENGINE = Memory;
CREATE STREAM t3_00848 ( id nullable(string), not_id nullable(string) ) ENGINE = Memory;

insert into t1_00848 (id) values ('l');
insert into t3_00848 (id) values ('r');

select sleep(3);
SELECT 'on';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;

SELECT 'using';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;

SET join_use_nulls = 1;

SELECT 'on + join_use_nulls';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;

SELECT 'using + join_use_nulls';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;

DROP STREAM t1_00848;
DROP STREAM t2_00848;
DROP STREAM t3_00848;
