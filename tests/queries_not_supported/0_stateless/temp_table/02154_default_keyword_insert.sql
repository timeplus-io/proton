CREATE TEMPORARY STREAM IF NOT EXISTS default_table (x uint32, y uint32 DEFAULT 42, z uint32 DEFAULT 33) ;

INSERT INTO default_table(x) values (DEFAULT);
INSERT INTO default_table(x, z) values (1, DEFAULT);
INSERT INTO default_table values (2, 33, DEFAULT);

SELECT * FROM default_table ORDER BY x;
