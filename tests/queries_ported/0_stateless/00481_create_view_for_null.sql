SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS null_00481;
DROP STREAM IF EXISTS null_view;

create stream null_00481 (x uint8) ENGINE = Null;
CREATE VIEW null_view AS SELECT * FROM null_00481;
INSERT INTO null_00481 VALUES (1);
SELECT sleep(3);

SELECT * FROM null_00481;
SELECT * FROM null_view;

DROP STREAM null_00481;
DROP STREAM null_view;
