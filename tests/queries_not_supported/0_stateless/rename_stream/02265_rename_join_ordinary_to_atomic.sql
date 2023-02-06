-- Tags: no-parallel


DROP DATABASE IF EXISTS 02265_atomic_db;
DROP DATABASE IF EXISTS 02265_ordinary_db;

CREATE DATABASE 02265_atomic_db ENGINE = Atomic;
CREATE DATABASE 02265_ordinary_db ENGINE = Ordinary;

CREATE STREAM 02265_ordinary_db.join_table ( `a` int64 ) ENGINE = Join(`ALL`, LEFT, a);
INSERT INTO 02265_ordinary_db.join_table VALUES (111);

RENAME STREAM 02265_ordinary_db.join_table TO 02265_atomic_db.join_table;

SELECT * FROM 02265_atomic_db.join_table;

DROP DATABASE IF EXISTS 02265_atomic_db;
DROP DATABASE IF EXISTS 02265_ordinary_db;
