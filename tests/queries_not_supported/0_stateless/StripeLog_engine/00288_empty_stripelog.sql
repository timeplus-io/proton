SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS stripelog;
create stream stripelog (x uint8) ENGINE = StripeLog;

SELECT * FROM stripelog ORDER BY x;
INSERT INTO stripelog VALUES (1), (2);

SELECT sleep(3);

SELECT * FROM stripelog ORDER BY x;

DROP STREAM stripelog;
