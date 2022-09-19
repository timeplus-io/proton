SELECT 'hello' UNION ALL SELECT toLowCardinality('hello');
SELECT to_type_name(x) FROM (SELECT 'hello' AS x UNION ALL SELECT toLowCardinality('hello'));

SELECT '---';

create temporary table t1(a string);
create temporary table t2(a LowCardinality(string));
select a from t1 union all select a from t2;

SELECT '---';

CREATE TEMPORARY STREAM a (x string);
CREATE TEMPORARY STREAM b (x LowCardinality(string));
CREATE TEMPORARY STREAM c (x Nullable(string));
CREATE TEMPORARY STREAM d (x LowCardinality(Nullable(string)));

INSERT INTO a VALUES ('hello');
INSERT INTO b VALUES ('hello');
INSERT INTO c VALUES ('hello');
INSERT INTO d VALUES ('hello');

SELECT x FROM a;
SELECT x FROM b;
SELECT x FROM c;
SELECT x FROM d;

SELECT '---';

SELECT x FROM a UNION ALL SELECT x FROM b;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM c;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM b UNION ALL SELECT x FROM a;
SELECT '-';
SELECT x FROM b UNION ALL SELECT x FROM c;
SELECT '-';
SELECT x FROM b UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM c UNION ALL SELECT x FROM a;
SELECT '-';
SELECT x FROM c UNION ALL SELECT x FROM b;
SELECT '-';
SELECT x FROM c UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM d UNION ALL SELECT x FROM a;
SELECT '-';
SELECT x FROM d UNION ALL SELECT x FROM c;
SELECT '-';
SELECT x FROM d UNION ALL SELECT x FROM b;

SELECT '---';

SELECT x FROM b UNION ALL SELECT x FROM c UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM c UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c;

SELECT '---';

SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c UNION ALL SELECT x FROM d;

SELECT '---';

SELECT [CAST('abc' AS LowCardinality(string)), CAST('def' AS Nullable(string))];
SELECT [CAST('abc' AS LowCardinality(string)), CAST('def' AS FixedString(3))];
SELECT [CAST('abc' AS LowCardinality(string)), CAST('def' AS LowCardinality(FixedString(3)))];
