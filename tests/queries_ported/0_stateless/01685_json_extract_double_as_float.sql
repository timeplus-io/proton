-- Tags: no-fasttest

WITH '{ "v":1.1}' AS raw
SELECT
    json_extract(raw, 'v', 'float') AS float32_1,
    json_extract(raw, 'v', 'float32') AS float32_2,
    json_extract_float(raw, 'v') AS float64_1,
    json_extract(raw, 'v', 'double') AS float64_2;

WITH '{ "v":1E-2}' AS raw
SELECT
    json_extract(raw, 'v', 'float') AS float32_1,
    json_extract(raw, 'v', 'float32') AS float32_2,
    json_extract_float(raw, 'v') AS float64_1,
    json_extract(raw, 'v', 'double') AS float64_2;

SELECT json_extract('{"v":1.1}', 'v', 'uint64');
SELECT json_extract('{"v":1.1}', 'v', 'nullable(uint64)');

SELECT json_extract('{"v":-1e300}', 'v', 'float64');
SELECT json_extract('{"v":-1e300}', 'v', 'float32');

SELECT json_extract('{"v":-1e300}', 'v', 'uint64');
SELECT json_extract('{"v":-1e300}', 'v', 'int64');
SELECT json_extract('{"v":-1e300}', 'v', 'uint8');
SELECT json_extract('{"v":-1e300}', 'v', 'int8');
