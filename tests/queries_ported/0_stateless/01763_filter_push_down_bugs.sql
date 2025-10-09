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

DROP STREAM IF EXISTS t;
create stream t(a uint8) engine=MergeTree order by a;
insert into t select * from numbers(2);
select a from t as t1 join t as t2 on t1.a = t2.a where t1.a;
DROP STREAM IF EXISTS t;

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
CREATE STREAM t1 (id int64, create_time datetime) ENGINE = MergeTree ORDER BY id;
CREATE STREAM t2 (delete_time datetime) ENGINE = MergeTree ORDER BY delete_time;

insert into t1 values (101, '2023-05-28 00:00:00'), (102, '2023-05-28 00:00:00');
insert into t2 values ('2023-05-31 00:00:00');

EXPLAIN indexes=1 SELECT id, delete_time FROM t1
 CROSS JOIN (
    SELECT delete_time
    FROM t2
) AS d WHERE create_time < delete_time AND id = 101 SETTINGS allow_experimental_analyzer=0;

EXPLAIN indexes=1 SELECT id, delete_time FROM t1
 CROSS JOIN (
    SELECT delete_time
    FROM t2
) AS d WHERE create_time < delete_time AND id = 101 SETTINGS allow_experimental_analyzer=1;

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

-- expected to get row (1, 3, 1, 4) from JOIN and empty result from the query
SELECT *
FROM
(
    SELECT *
    FROM Values('id uint64, t uint64', (1, 3))
) AS t1
ASOF INNER JOIN
(
    SELECT *
    FROM Values('id uint64, t uint64', (1, 1), (1, 2), (1, 3), (1, 4), (1, 5))
) AS t2 ON (t1.id = t2.id) AND (t1.t < t2.t)
WHERE t2.t != 4;