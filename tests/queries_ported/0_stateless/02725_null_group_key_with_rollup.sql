set allow_suspicious_low_cardinality_types=1;
DROP STREAM IF EXISTS group_by_null_key;
CREATE STREAM group_by_null_key (c1 nullable(int32), c2 low_cardinality(nullable(int32))) ENGINE = Memory();
INSERT INTO group_by_null_key VALUES (null, null), (null, null);

select c1, count(*) from group_by_null_key group by c1 WITH TOTALS;
select c2, count(*) from group_by_null_key group by c2 WITH TOTALS;

select c1, count(*) from group_by_null_key group by ROLLUP(c1);
select c2, count(*) from group_by_null_key group by ROLLUP(c2);


DROP STREAM group_by_null_key;
