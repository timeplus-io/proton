-- Tags: no-fasttest
SELECT json_extract('{"a": 123456}', 'fixed_string(11)');
SELECT json_extract('{"a": 123456}', 'fixed_string(12)');
SELECT json_extract('{"a": "123456"}', 'a', 'fixed_string(5)');
SELECT json_extract('{"a": "123456"}', 'a', 'fixed_string(6)');
SELECT json_extract('{"a": 123456}', 'a', 'fixed_string(5)');
SELECT json_extract('{"a": 123456}', 'a', 'fixed_string(6)');
SELECT json_extract(materialize('{"a": 131231}'), 'a', 'low_cardinality(fixed_string(5))') FROM numbers(2);
SELECT json_extract(materialize('{"a": 131231}'), 'a', 'low_cardinality(fixed_string(6))') FROM numbers(2);
