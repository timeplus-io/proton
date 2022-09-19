SET any_join_distinct_right_table_keys = 1;

DROP STREAM IF EXISTS t1_00848;
DROP STREAM IF EXISTS t2_00848;
DROP STREAM IF EXISTS t3_00848;
create stream t1_00848 ( id string ) ;
create stream t2_00848 ( id Nullable(string) ) ;
create stream t3_00848 ( id Nullable(string), not_id Nullable(string) ) ;

insert into t1_00848 values ('l');
insert into t3_00848 (id) values ('r');

SELECT 'on';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;

SELECT 'using';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;

SET join_use_nulls = 1;

SELECT 'on + join_use_nulls';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;

SELECT 'using + join_use_nulls';

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t1.id), to_type_name(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, to_type_name(t2.id), to_type_name(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;

DROP STREAM t1_00848;
DROP STREAM t2_00848;
DROP STREAM t3_00848;
