-- Tags: no-parallel

DROP STREAM IF EXISTS 02483_substitute_udf;
DROP FUNCTION IF EXISTS 02483_plusone;
DROP FUNCTION IF EXISTS 02483_plustwo;
DROP FUNCTION IF EXISTS 02483_plusthree;

-- { echo }
CREATE FUNCTION 02483_plusone AS (a) -> a + 1;
CREATE STREAM 02483_substitute_udf (id uint32, number uint32 DEFAULT 02483_plusone(id)) ENGINE=MergeTree() ORDER BY id;
DESC STREAM 02483_substitute_udf;
INSERT INTO 02483_substitute_udf (id, number) VALUES (1, NULL);
SELECT * FROM 02483_substitute_udf ORDER BY id;

CREATE FUNCTION 02483_plustwo AS (a) -> a + 2;
ALTER STREAM 02483_substitute_udf MODIFY COLUMN number uint32 DEFAULT 02483_plustwo(id);
DESC STREAM 02483_substitute_udf;
INSERT INTO 02483_substitute_udf (id, number) VALUES (5, NULL);
SELECT * FROM 02483_substitute_udf ORDER BY id;

CREATE FUNCTION 02483_plusthree AS (a) -> a + 3;
ALTER STREAM 02483_substitute_udf DROP COLUMN number;
ALTER STREAM 02483_substitute_udf ADD COLUMN new_number uint32 DEFAULT 02483_plusthree(id);
DESC STREAM 02483_substitute_udf;
INSERT INTO 02483_substitute_udf (id, new_number) VALUES (10, NULL);
SELECT * FROM 02483_substitute_udf ORDER BY id;

DROP STREAM 02483_substitute_udf;
DROP FUNCTION 02483_plusone;
DROP FUNCTION 02483_plustwo;
DROP FUNCTION 02483_plusthree;
