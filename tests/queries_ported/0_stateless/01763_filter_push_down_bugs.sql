SELECT * FROM (SELECT col1, col2 FROM (select '1' as col1, '2' as col2) GROUP by col1, col2) AS expr_qry WHERE col2 != '';
SELECT * FROM (SELECT materialize('1') AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';
SELECT * FROM (SELECT materialize([1]) AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';
SELECT * FROM (SELECT materialize([[1]]) AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';

DROP STREAM IF EXISTS Test;

CREATE STREAM Test
ENGINE = MergeTree()
PRIMARY KEY (string1,string2)
ORDER BY (string1,string2)
AS
SELECT
   'string1_' || to_string(number) as string1,
   'string2_' || to_string(number) as string2,
   'string3_' || to_string(number) as string3,
   'string4_' || to_string(number%4) as string4
FROM numbers(1);

SELECT *
FROM
  (
   SELECT string1,string2,string3,string4,count(*)
   FROM Test
   GROUP by string1,string2,string3,string4
  ) AS expr_qry;

SELECT *
FROM
  (
    SELECT string1,string2,string3,string4,count(*)
    FROM Test
    GROUP by string1,string2,string3,string4
  ) AS expr_qry
WHERE  string4 ='string4_0';

DROP STREAM IF EXISTS Test;

select x, y from (select [0, 1, 2] as y, 1 as a, 2 as b) array join y as x where a = 1 and b = 2 and (x = 1 or x != 1) and x = 1;

create stream t(a uint8) engine=MergeTree order by a;
insert into t select * from numbers(2);
select a from t t1 join t t2 on t1.a = t2.a where t1.a;
