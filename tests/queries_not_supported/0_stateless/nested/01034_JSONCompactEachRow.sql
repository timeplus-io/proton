DROP STREAM IF EXISTS test_table;
DROP STREAM IF EXISTS test_table_2;
SET input_format_null_as_default = 0;
SELECT 1;
/* Check JSONCompactEachRow Output */
create stream test_table (value uint8, name string) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM test_table FORMAT JSONCompactEachRow;
SELECT 2;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactEachRow;
SELECT 3;
/* Check JSONCompactEachRowWithNames and JSONCompactEachRowWithNamesAndTypes Output */
SELECT * FROM test_table FORMAT JSONCompactEachRowWithNamesAndTypes;
SELECT '----------';
SELECT * FROM test_table FORMAT JSONCompactEachRowWithNames;
SELECT 4;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactEachRowWithNamesAndTypes;
DROP STREAM IF EXISTS test_table;
SELECT 5;
/* Check JSONCompactEachRow Input */
create stream test_table (v1 string, v2 uint8, v3 DEFAULT v2 * 16, v4 uint8 DEFAULT 8) ENGINE = MergeTree() ORDER BY v2;
INSERT INTO test_table FORMAT JSONCompactEachRow ["first", 1, "2", null] ["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
TRUNCATE TABLE test_table;
SELECT 6;
/* Check input_format_null_as_default = 1 */
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONCompactEachRow ["first", 1, "2", null] ["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
TRUNCATE TABLE test_table;
SELECT 7;
/* Check nested */
create stream test_table_2 (v1 uint8, n nested(id uint8, name string)) ENGINE = MergeTree() ORDER BY v1;
INSERT INTO test_table_2 FORMAT JSONCompactEachRow [16, [15, 16, null], ["first", "second", "third"]];
SELECT * FROM test_table_2 FORMAT JSONCompactEachRow;
TRUNCATE TABLE test_table_2;
SELECT 8;
/* Check JSONCompactEachRowWithNamesAndTypes and JSONCompactEachRowWithNamesAndTypes Input */
SET input_format_null_as_default = 0;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "v2", "v3", "v4"]["string","uint8","UInt16","uint8"]["first", 1, "2", null]["second", 2, null, 6];
INSERT INTO test_table FORMAT JSONCompactEachRowWithNames ["v1", "v2", "v3", "v4"]["first", 1, "2", null]["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
TRUNCATE TABLE test_table;
SELECT 9;
/* Check input_format_null_as_default = 1 */
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "v2", "v3", "v4"]["string","uint8","UInt16","uint8"]["first", 1, "2", null] ["second", 2, null, 6];
INSERT INTO test_table FORMAT JSONCompactEachRowWithNames ["v1", "v2", "v3", "v4"]["first", 1, "2", null] ["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
SELECT 10;
/* Check Header */
TRUNCATE TABLE test_table;
SET input_format_skip_unknown_fields = 1;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "v2", "invalid_column"]["string", "uint8", "uint8"]["first", 1, 32]["second", 2, "64"];
INSERT INTO test_table FORMAT JSONCompactEachRowWithNames ["v1", "v2", "invalid_column"]["first", 1, 32]["second", 2, "64"];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
SELECT 11;
TRUNCATE TABLE test_table;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v4", "v2", "v3"]["uint8", "uint8", "UInt16"][1, 2, 3]
INSERT INTO test_table FORMAT JSONCompactEachRowWithNames ["v4", "v2", "v3"][1, 2, 3]
SELECT * FROM test_table FORMAT JSONCompactEachRowWithNamesAndTypes;
SELECT '----------';
SELECT * FROM test_table FORMAT JSONCompactEachRowWithNames;
SELECT 12;
/* Check nested */
INSERT INTO test_table_2 FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "n.id", "n.name"]["uint8", "array(uint8)", "array(string)"][16, [15, 16, null], ["first", "second", "third"]];
INSERT INTO test_table_2 FORMAT JSONCompactEachRowWithNames ["v1", "n.id", "n.name"][16, [15, 16, null], ["first", "second", "third"]];
SELECT * FROM test_table_2 FORMAT JSONCompactEachRowWithNamesAndTypes;
SELECT '----------';
SELECT * FROM test_table_2 FORMAT JSONCompactEachRowWithNames;

DROP STREAM IF EXISTS test_table;
DROP STREAM IF EXISTS test_table_2;
