DROP STREAM IF EXISTS file;
create stream file (s string, n uint32) ENGINE = File(CSVWithNames);
-- BTW, WithNames formats are totally unsuitable for more than a single INSERT
INSERT INTO file VALUES ('hello', 1), ('world', 2);

SELECT * FROM file;
CREATE TEMPORARY STREAM file2 AS SELECT * FROM file;
SELECT * FROM file2;

DROP STREAM file;
