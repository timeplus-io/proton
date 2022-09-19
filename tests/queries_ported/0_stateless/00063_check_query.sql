SET check_query_single_value_result = 1;
SET query_mode = 'table';
DROP STREAM IF EXISTS check_query_tiny_log;

create stream check_query_tiny_log (N uint32, S string);

INSERT INTO check_query_tiny_log(N, S) VALUES (1, 'A'), (2, 'B'), (3, 'C');


CHECK TABLE check_query_tiny_log;


DROP STREAM IF EXISTS check_query_log;

create stream check_query_log (N uint32,S string);

INSERT INTO check_query_log(N, S) VALUES (1, 'A'), (2, 'B'), (3, 'C');

CHECK TABLE check_query_log;

DROP STREAM check_query_log;
DROP STREAM check_query_tiny_log;
