-- Tags: no-fasttest

WITH '{ "v":1.1}' AS raw
SELECT
    JSONExtract(raw, 'v', 'float') AS float32_1,
    JSONExtract(raw, 'v', 'Float32') AS float32_2,
    JSONExtractFloat(raw, 'v') AS float64_1,
    JSONExtract(raw, 'v', 'double') AS float64_2;

WITH '{ "v":1E-2}' AS raw
SELECT
    JSONExtract(raw, 'v', 'float') AS float32_1,
    JSONExtract(raw, 'v', 'Float32') AS float32_2,
    JSONExtractFloat(raw, 'v') AS float64_1,
    JSONExtract(raw, 'v', 'double') AS float64_2;

SELECT JSONExtract('{"v":1.1}', 'v', 'uint64');
SELECT JSONExtract('{"v":1.1}', 'v', 'Nullable(uint64)');

SELECT JSONExtract('{"v":-1e300}', 'v', 'float64');
SELECT JSONExtract('{"v":-1e300}', 'v', 'Float32');

SELECT JSONExtract('{"v":-1e300}', 'v', 'uint64');
SELECT JSONExtract('{"v":-1e300}', 'v', 'int64');
SELECT JSONExtract('{"v":-1e300}', 'v', 'uint8');
SELECT JSONExtract('{"v":-1e300}', 'v', 'int8');
