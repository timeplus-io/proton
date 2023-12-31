DROP STREAM IF EXISTS tbl;

SET allow_suspicious_low_cardinality_types = 1;
CREATE STREAM tbl (`lc` low_cardinality(uuid)) ENGINE = Memory;

INSERT INTO tbl VALUES ('0562380c-d1f3-4091-83d5-8c972f534317');

SET extremes = 1;
SELECT * FROM tbl;

DROP STREAM tbl;
