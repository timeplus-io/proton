DROP STREAM IF EXISTS alter_00147;

create stream alter_00147 (d date DEFAULT to_date('2015-01-01'), n nested(x string)) ENGINE = MergeTree(d, d, 8192);

INSERT INTO alter_00147 (`n.x`) VALUES (['Hello', 'World']);

SELECT * FROM alter_00147;
SELECT * FROM alter_00147 ARRAY JOIN n;
SELECT * FROM alter_00147 ARRAY JOIN n WHERE n.x LIKE '%Hello%';

ALTER STREAM alter_00147 ADD COLUMN n.y array(uint64);

SELECT * FROM alter_00147;
SELECT * FROM alter_00147 ARRAY JOIN n;
SELECT * FROM alter_00147 ARRAY JOIN n WHERE n.x LIKE '%Hello%';

INSERT INTO alter_00147 (`n.x`) VALUES (['Hello2', 'World2']);

SELECT * FROM alter_00147 ORDER BY n.x;
SELECT * FROM alter_00147 ARRAY JOIN n ORDER BY n.x;
SELECT * FROM alter_00147 ARRAY JOIN n WHERE n.x LIKE '%Hello%' ORDER BY n.x;

OPTIMIZE STREAM alter_00147;

SELECT * FROM alter_00147;
SELECT * FROM alter_00147 ARRAY JOIN n;
SELECT * FROM alter_00147 ARRAY JOIN n WHERE n.x LIKE '%Hello%';

DROP STREAM alter_00147;
