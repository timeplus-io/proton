DROP STREAM IF EXISTS mass_table_312;
create stream mass_table_312 (d date DEFAULT '2000-01-01', x uint64, n nested(a string, b string)) ENGINE = MergeTree(d, x, 1);
INSERT INTO mass_table_312 SELECT * FROM generateRandom('`d` date,`x` uint64,`n.a` array(string),`n.b` array(string)', 1, 10, 2) LIMIT 100;

SELECT count(), sum(cityHash64(*)) FROM mass_table_312;
SELECT count(), sum(cityHash64(*)) FROM mass_table_312 ARRAY JOIN n;

DROP STREAM mass_table_312;
