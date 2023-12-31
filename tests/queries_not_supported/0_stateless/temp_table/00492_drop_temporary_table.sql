SET query_mode = 'table';
DROP TEMPORARY STREAM IF EXISTS temp_tab;
CREATE TEMPORARY STREAM temp_tab (number uint64);
INSERT INTO temp_tab SELECT number FROM system.numbers LIMIT 1;
SELECT number FROM temp_tab;
SET send_logs_level = 'fatal';
EXISTS TEMPORARY TABLE temp_tab;
drop stream temp_tab;
EXISTS TEMPORARY TABLE temp_tab;
SET send_logs_level = 'warning';
CREATE TEMPORARY STREAM temp_tab (number uint64);
SELECT number FROM temp_tab;
DROP TEMPORARY TABLE temp_tab;
