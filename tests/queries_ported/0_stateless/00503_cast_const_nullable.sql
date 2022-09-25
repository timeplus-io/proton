SELECT CAST(1 AS nullable(uint8)) AS id WHERE id = CAST(1 AS nullable(uint8));
SELECT CAST(1 AS nullable(uint8)) AS id WHERE id = 1;
SELECT NULL == CAST(to_uint8(0) AS nullable(uint8));
