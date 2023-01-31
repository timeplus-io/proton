<<<<<<< HEAD:tests/queries/0_stateless/00939_test_null_in.sql
DROP TABLE IF EXISTS nullt;

CREATE TABLE nullt (c1 Nullable(UInt32), c2 Nullable(String))ENGINE = Log;
INSERT INTO nullt VALUES (1, 'abc'), (2, NULL), (NULL, NULL);
=======
SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS nullt;

create stream nullt (c1 nullable(uint32), c2 nullable(string)) ;
INSERT INTO nullt(c1,c2) VALUES (1, 'abc'), (2, NULL), (NULL, NULL);
>>>>>>> 9e73b005c8... CH porting case ,v3:tests/queries_ported/0_stateless/00939_test_null_in.sql

SELECT sleep(3);
SELECT c2 = ('abc') FROM nullt;
SELECT c2 IN ('abc') FROM nullt;

SELECT c2 IN ('abc', NULL) FROM nullt;

DROP TABLE IF EXISTS nullt;
