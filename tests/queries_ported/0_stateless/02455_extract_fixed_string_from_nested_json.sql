-- Tags: no-fasttest
DROP STREAM IF EXISTS test_fixed_string_nested_json;
CREATE STREAM test_fixed_string_nested_json (data string) ENGINE MergeTree ORDER BY data;
INSERT INTO test_fixed_string_nested_json (data) VALUES ('{"a" : {"b" : {"c" : 1, "d" : "str"}}}');
SELECT json_extract(data, 'tuple(a fixed_string(24))') AS json FROM test_fixed_string_nested_json;
DROP STREAM test_fixed_string_nested_json;