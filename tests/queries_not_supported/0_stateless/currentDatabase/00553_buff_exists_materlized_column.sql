DROP STREAM IF EXISTS nums;
DROP STREAM IF EXISTS nums_buf;

SET insert_allow_materialized_columns = 1;

create stream nums ( n uint64, m uint64 MATERIALIZED n+1 )  ;
create stream nums_buf AS nums ENGINE = Buffer(currentDatabase(), nums, 1, 10, 100, 1, 3, 10000000, 100000000);

INSERT INTO nums_buf (n) VALUES (1);
INSERT INTO nums_buf (n) VALUES (2);
INSERT INTO nums_buf (n) VALUES (3);
INSERT INTO nums_buf (n) VALUES (4);
INSERT INTO nums_buf (n) VALUES (5);

SELECT n,m FROM nums ORDER BY n;
SELECT n,m FROM nums_buf ORDER BY n;

DROP STREAM IF EXISTS nums_buf;
DROP STREAM IF EXISTS nums;
