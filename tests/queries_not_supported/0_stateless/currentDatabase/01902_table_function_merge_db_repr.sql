-- Tags: no-parallel

DROP DATABASE IF EXISTS 01902_db;
DROP DATABASE IF EXISTS 01902_db1;
DROP DATABASE IF EXISTS 01902_db2;
DROP DATABASE IF EXISTS 01902_db3;

CREATE DATABASE 01902_db;
CREATE DATABASE 01902_db1;
CREATE DATABASE 01902_db2;
CREATE DATABASE 01902_db3;

create stream 01902_db.t   (n int8) ENGINE=MergeTree ORDER BY n;
create stream 01902_db1.t1 (n int8) ENGINE=MergeTree ORDER BY n;
create stream 01902_db2.t2 (n int8) ENGINE=MergeTree ORDER BY n;
create stream 01902_db3.t3 (n int8) ENGINE=MergeTree ORDER BY n;

INSERT INTO 01902_db.t   SELECT * FROM numbers(10);
INSERT INTO 01902_db1.t1 SELECT * FROM numbers(10);
INSERT INTO 01902_db2.t2 SELECT * FROM numbers(10);
INSERT INTO 01902_db3.t3 SELECT * FROM numbers(10);

SELECT 'create stream t_merge as 01902_db.t ENGINE=Merge(REGEXP(^01902_db), ^t)';
create stream 01902_db.t_merge as 01902_db.t ENGINE=Merge(REGEXP('^01902_db'), '^t');

SELECT 'SELECT _database, _table, n FROM 01902_db.t_merge ORDER BY _database, _table, n';
SELECT _database, _table, n FROM 01902_db.t_merge ORDER BY _database, _table, n;

SELECT 'SHOW create stream 01902_db.t_merge';
SHOW CREATE  TABLE 01902_db.t_merge;

SELECT 'SELECT _database, _table, n FROM merge(REGEXP(^01902_db), ^t) ORDER BY _database, _table, n';
SELECT _database, _table, n FROM merge(REGEXP('^01902_db'), '^t') ORDER BY _database, _table, n;

SELECT 'SELECT _database, _table, n FROM 01902_db.t_merge WHERE _database = 01902_db1 ORDER BY _database, _table, n';
SELECT _database, _table, n FROM 01902_db.t_merge WHERE _database = '01902_db1' ORDER BY _database, _table, n;

SELECT 'SELECT _database, _table, n FROM 01902_db.t_merge WHERE _table = t1 ORDER BY _database, _table, n';
SELECT _database, _table, n FROM 01902_db.t_merge WHERE _table = 't1' ORDER BY _database, _table, n;

-- not regexp
SELECT 'create stream t_merge1 as 01902_db.t ENGINE=Merge(01902_db, ^t$)';
create stream 01902_db.t_merge1 as 01902_db.t ENGINE=Merge('01902_db', '^t$');

SELECT 'SELECT _database, _table, n FROM 01902_db.t_merge1 ORDER BY _database, _table, n';
SELECT _database, _table, n FROM 01902_db.t_merge1 ORDER BY _database, _table, n;

SELECT 'SELECT _database, _table, n FROM merge(01902_db, ^t$) ORDER BY _database, _table, n';
SELECT _database, _table, n FROM merge('01902_db', '^t$') ORDER BY _database, _table, n;

USE 01902_db1;

SELECT 'create stream t_merge_1 as 01902_db.t ENGINE=Merge(currentDatabase(), ^t)';
create stream 01902_db.t_merge_1 as 01902_db.t ENGINE=Merge(currentDatabase(), '^t');

SELECT 'SELECT _database, _table, n FROM 01902_db.t_merge_1 ORDER BY _database, _table, n';
SELECT _database, _table, n FROM 01902_db.t_merge_1 ORDER BY _database, _table, n;

SELECT 'SHOW create stream 01902_db.t_merge_1';
SHOW create stream 01902_db.t_merge_1;

SELECT 'SELECT _database, _table, n FROM merge(currentDatabase(), ^t) ORDER BY _database, _table, n';
SELECT _database, _table, n FROM merge(currentDatabase(), '^t') ORDER BY _database, _table, n;

DROP DATABASE 01902_db;
DROP DATABASE 01902_db1;
DROP DATABASE 01902_db2;
DROP DATABASE 01902_db3;
