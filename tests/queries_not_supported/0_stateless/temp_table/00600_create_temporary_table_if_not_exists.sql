CREATE TEMPORARY STREAM IF NOT EXISTS temporary_table (column uint32) ;
CREATE TEMPORARY STREAM IF NOT EXISTS temporary_table (column uint32) ;
INSERT INTO temporary_table VALUES (1), (2), (3);
SELECT column FROM temporary_table ORDER BY column;