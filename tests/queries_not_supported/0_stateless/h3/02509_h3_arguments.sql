-- Tags: no-fasttest

select h3ToParent(641573946153969375, 1);
select h3ToParent(641573946153969375, array_join([1,2]));

DROP STREAM IF EXISTS data_table;

CREATE STREAM data_table (id uint64, longitude float64, latitude float64) ENGINE=MergeTree ORDER BY id;
INSERT INTO data_table SELECT number, number, number FROM numbers(10);
SELECT geoToH3(longitude,  latitude, to_uint8(8)) AS h3Index FROM data_table ORDER BY 1;
SELECT geoToH3(longitude,  latitude, to_uint8(longitude - longitude + 8)) AS h3Index FROM data_table ORDER BY 1;

DROP STREAM data_table;
