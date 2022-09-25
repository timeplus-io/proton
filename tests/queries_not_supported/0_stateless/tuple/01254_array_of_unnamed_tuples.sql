DROP STREAM IF EXISTS mass_table_457;
create stream mass_table_457 (key array(tuple(float64, float64)), name string, value uint64) ;
INSERT INTO mass_table_457 SELECT * FROM generateRandom('`key` array(tuple(float64, float64)),`name` string,`value` uint64', 1, 10, 2) LIMIT 10;
SELECT * FROM mass_table_457;
DROP STREAM mass_table_457;
