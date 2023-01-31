-- Tags: no-fasttest

WITH '{"a": "hello", "b": 12345678901234567890}' AS json
SELECT json_extract_raw(json, 'a');
