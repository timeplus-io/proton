SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS null_00557;

create stream null_00557 (x uint8) ENGINE = Null;
DESCRIBE stream null_00557;

ALTER STREAM null_00557 ADD COLUMN y string, MODIFY COLUMN x int64 DEFAULT to_int64(y);
DESCRIBE stream null_00557;

DROP STREAM null_00557;
