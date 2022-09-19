DROP STREAM IF EXISTS file;
create stream file (number uint64) ENGINE = File(TSV);
SELECT * FROM file; -- { serverError 107 }
INSERT INTO file VALUES (1);
SELECT * FROM file;
DROP STREAM file;
