-- Tags: no-parallel

DROP STREAM IF EXISTS data_01269;
CREATE STREAM data_01269
(
    key     int32,
    value   nullable(int32),
    alias   uint8 ALIAS value>0
)
ENGINE = MergeTree()
ORDER BY key;
INSERT INTO data_01269 VALUES (1, 0);

-- after PR#10441
SELECT to_type_name(alias) FROM data_01269;
SELECT any(alias) FROM data_01269;

-- even without PR#10441
ALTER STREAM data_01269 DROP COLUMN alias;
ALTER STREAM data_01269 ADD COLUMN alias uint8 ALIAS value>0;
SELECT to_type_name(alias) FROM data_01269;
SELECT any(alias) FROM data_01269;

DROP STREAM data_01269;
