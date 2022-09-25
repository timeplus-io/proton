DROP STREAM IF EXISTS test_prewhere_default_column;
DROP STREAM IF EXISTS test_prewhere_column_type;

create stream test_prewhere_default_column (APIKey uint8, SessionType uint8) ENGINE = MergeTree() PARTITION BY APIKey ORDER BY tuple();
INSERT INTO test_prewhere_default_column VALUES( 42, 42 );
ALTER STREAM test_prewhere_default_column ADD COLUMN OperatingSystem uint64 DEFAULT SessionType+1;

SELECT OperatingSystem FROM test_prewhere_default_column PREWHERE SessionType = 42;


create stream test_prewhere_column_type (`a` low_cardinality(string), `x` Nullable(int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_prewhere_column_type VALUES ('', 2);

SELECT a, y FROM test_prewhere_column_type prewhere (x = 2) AS y;
SELECT a, to_type_name(x = 2), to_type_name(x) FROM test_prewhere_column_type where (x = 2) AS y;

DROP STREAM test_prewhere_default_column;
DROP STREAM test_prewhere_column_type;
