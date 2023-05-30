drop stream if exists t;
create stream t (a int8, val float32) engine = Memory();
insert into t values (1,1.1), (1,1.2), (2,2.1);

SET enable_optimize_predicate_expression = 0;

SELECT * FROM (
    SELECT a, t1.val as val1, t2.val as val2
    FROM t as t1
    ANY LEFT JOIN t as t2 USING a
) ORDER BY val1;

SET enable_optimize_predicate_expression = 1;

SELECT * FROM (
    SELECT a, t1.val as val1, t2.val as val2
    FROM t as t1
    ANY LEFT JOIN t as t2 USING a
) ORDER BY val1;

drop stream t;
