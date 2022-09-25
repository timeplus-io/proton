DROP STREAM IF EXISTS mass_table_117;
create stream mass_table_117 (`dt` date, `site_id` int32, `site_key` string) ENGINE = MergeTree(dt, (site_id, site_key, dt), 8192);
INSERT INTO mass_table_117 SELECT * FROM generateRandom('`dt` date,`site_id` int32,`site_key` string', 1, 10, 2) LIMIT 100;
SELECT count(), sum(cityHash64(*)) FROM mass_table_117;
DROP STREAM mass_table_117;
