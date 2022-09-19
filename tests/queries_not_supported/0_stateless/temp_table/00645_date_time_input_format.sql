CREATE TEMPORARY STREAM test_00645 (d DateTime) ;
SET date_time_input_format = 'best_effort';
INSERT INTO test_00645 VALUES ('2018-06-08T01:02:03.000Z');
SELECT toTimeZone(d, 'UTC') FROM test_00645;
