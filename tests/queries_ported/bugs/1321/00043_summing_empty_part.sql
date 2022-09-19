SET query_mode = 'table';
DROP STREAM IF EXISTS empty_summing;
create stream empty_summing (d date, k uint64, v int8);

INSERT INTO empty_summing(d, k, v) VALUES ('2015-01-01', 1, 10);
INSERT INTO empty_summing (d, k, v) VALUES ('2015-01-01', 1, -10);
SELECT sleep(3);
OPTIMIZE TABLE empty_summing;
SELECT d,k,v FROM empty_summing;
INSERT INTO empty_summing(d, k, v) VALUES ('2015-01-01', 1, 4),('2015-01-01', 2, -9),('2015-01-01', 3, -14);
INSERT INTO empty_summing(d, k, v) VALUES ('2015-01-01', 1, -2),('2015-01-01', 1, -2),('2015-01-01', 3, 14);
INSERT INTO empty_summing(d, k, v) VALUES ('2015-01-01', 1, 0),('2015-01-01', 3, 0);
SELECT sleep(3);
OPTIMIZE TABLE empty_summing;
SELECT d,k,v FROM empty_summing;

DROP STREAM empty_summing;
