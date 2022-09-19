DROP STREAM IF EXISTS empty_summing;
create stream empty_summing (d date, k uint64, v int8) ENGINE=SummingMergeTree(d, k, 8192);

INSERT INTO empty_summing VALUES ('2015-01-01', 1, 10);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, -10);

OPTIMIZE STREAM empty_summing;
SELECT * FROM empty_summing;

INSERT INTO empty_summing VALUES ('2015-01-01', 1, 4),('2015-01-01', 2, -9),('2015-01-01', 3, -14);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, -2),('2015-01-01', 1, -2),('2015-01-01', 3, 14);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, 0),('2015-01-01', 3, 0);

OPTIMIZE STREAM empty_summing;
SELECT * FROM empty_summing;

DROP STREAM empty_summing;
