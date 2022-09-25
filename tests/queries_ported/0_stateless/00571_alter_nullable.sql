SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS nullable_00571;
create stream nullable_00571 (x string) ENGINE = MergeTree ORDER BY x;
INSERT INTO nullable_00571(x) VALUES ('hello'), ('world');
SELECT sleep(3);
SELECT * FROM nullable_00571;
ALTER STREAM nullable_00571 ADD COLUMN n nullable(uint64);
SELECT * FROM nullable_00571;
ALTER STREAM nullable_00571 DROP COLUMN n;
ALTER STREAM nullable_00571 ADD COLUMN n nullable(uint64) DEFAULT NULL;
SELECT * FROM nullable_00571;
ALTER STREAM nullable_00571 DROP COLUMN n;
ALTER STREAM nullable_00571 ADD COLUMN n nullable(uint64) DEFAULT 0;
SELECT * FROM nullable_00571;
DROP STREAM nullable_00571;
