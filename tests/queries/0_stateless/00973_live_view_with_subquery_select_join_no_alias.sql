-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS A;
DROP STREAM IF EXISTS B;

create stream A (id int32) Engine=Memory;
create stream B (id int32, name string) Engine=Memory;

CREATE LIVE VIEW lv AS SELECT id, name FROM ( SELECT A.id, B.name FROM A, B WHERE A.id = B.id);

SELECT * FROM lv;

INSERT INTO A VALUES (1);
INSERT INTO B VALUES (1, 'hello');

SELECT *,_version FROM lv ORDER BY id;
SELECT *,_version FROM lv ORDER BY id;

INSERT INTO A VALUES (2)
INSERT INTO B VALUES (2, 'hello')

SELECT *,_version FROM lv ORDER BY id;
SELECT *,_version FROM lv ORDER BY id;

DROP STREAM lv;
DROP STREAM A;
DROP STREAM B;
