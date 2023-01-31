set query_mode='table';
set asterisk_include_reserved_columns=false;
SET joined_subquery_requires_alias = 0;

SELECT ax, c FROM (SELECT [1,2] as ax, 0 as c) ARRAY JOIN ax JOIN (SELECT 0 as c) USING (c);
SELECT ax, c FROM (SELECT [3,4] as ax, 0 as c) JOIN (SELECT 0 as c) USING (c) ARRAY JOIN ax;
SELECT ax, c FROM (SELECT [5,6] as ax, 0 as c) s1 JOIN system.one s2 ON s1.c = s2.dummy ARRAY JOIN ax;
SELECT ax, c, d FROM (SELECT [7,8] as ax, 1 as c, 0 as d) s1 JOIN system.one s2 ON s1.c = s2.dummy OR s1.d = s2.dummy ARRAY JOIN ax;


SELECT ax, c FROM (SELECT [101,102] as ax, 0 as c) s1
JOIN system.one as s2 ON s1.c = s2.dummy
JOIN system.one as s3 ON s1.c = s3.dummy
ARRAY JOIN ax; -- { serverError 48 }

SELECT '-';

SET joined_subquery_requires_alias = 1;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;

create stream f (`d_ids` array(int64) ) ;
INSERT INTO f(d_ids) VALUES ([1, 2]);
select sleep(3);

CREATE TABLE d (`id` Int64, `name` String ) ENGINE = TinyLog;

INSERT INTO d(id,name) VALUES (2, 'a2'), (3, 'a3');
select sleep(3);

SELECT d_ids, id, name FROM f LEFT ARRAY JOIN d_ids LEFT JOIN d ON d.id = d_ids ORDER BY id;
SELECT did, id, name FROM f LEFT ARRAY JOIN d_ids as did LEFT JOIN d ON d.id = did ORDER BY id;

-- name clash, doesn't work yet
SELECT id, name FROM f LEFT ARRAY JOIN d_ids as id LEFT JOIN d ON d.id = id ORDER BY id; -- { serverError 403 }

SELECT * FROM ( SELECT [dummy, dummy] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN system.one AS y ON x.dummy == y.dummy;

SELECT * FROM ( SELECT [dummy, dummy] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN system.one AS y ON x.dummy + 1 == y.dummy + 1;

SELECT * FROM ( SELECT [dummy, dummy] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN system.one AS y USING dummy;

SELECT * FROM ( SELECT [toUInt32(dummy), toUInt32(dummy)] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN (select toInt32(dummy) as dummy from system.one ) AS y USING dummy;

SELECT dummy > 0, toTypeName(any(dummy)), any(toTypeName(dummy))
FROM ( SELECT [toUInt32(dummy), toUInt32(dummy)] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN ( SELECT toInt32(dummy) AS dummy FROM system.one ) AS y USING dummy GROUP BY (dummy > 0);

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;
