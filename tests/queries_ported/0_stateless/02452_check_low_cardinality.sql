-- Tags: no-fasttest
DROP STREAM IF EXISTS test_low_cardinality_string;
DROP STREAM IF EXISTS test_low_cardinality_uuid;
DROP STREAM IF EXISTS test_low_cardinality_int;
CREATE STREAM test_low_cardinality_string (data string) ENGINE MergeTree ORDER BY data;
CREATE STREAM test_low_cardinality_uuid (data string) ENGINE MergeTree ORDER BY data;
CREATE STREAM test_low_cardinality_int (data string) ENGINE MergeTree ORDER BY data;
INSERT INTO test_low_cardinality_string (data) VALUES ('{"a": "hi", "b": "hello", "c": "hola", "d": "see you, bye, bye"}');
INSERT INTO test_low_cardinality_int (data) VALUES ('{"a": 11, "b": 2222, "c": 33333333, "d": 4444444444444444}');
INSERT INTO test_low_cardinality_uuid (data) VALUES ('{"a": "2d49dc6e-ddce-4cd0-afb8-790956df54c4", "b": "2d49dc6e-ddce-4cd0-afb8-790956df54c3", "c": "2d49dc6e-ddce-4cd0-afb8-790956df54c1", "d": "2d49dc6e-ddce-4cd0-afb8-790956df54c1"}');
SELECT json_extract(data, 'tuple(
                            a low_cardinality(string),
                            b low_cardinality(string),
                            c low_cardinality(string),
                            d low_cardinality(string)
                            )') AS json FROM test_low_cardinality_string;
SELECT json_extract(data, 'tuple(
                            a low_cardinality(fixed_string(20)),
                            b low_cardinality(fixed_string(20)),
                            c low_cardinality(fixed_string(20)),
                            d low_cardinality(fixed_string(20))
                            )') AS json FROM test_low_cardinality_string;
SELECT json_extract(data, 'tuple(
                            a low_cardinality(int8),
                            b low_cardinality(int8),
                            c low_cardinality(int8),
                            d low_cardinality(int8)
                            )') AS json FROM test_low_cardinality_int;
SELECT json_extract(data, 'tuple(
                            a low_cardinality(int16),
                            b low_cardinality(int16),
                            c low_cardinality(int16),
                            d low_cardinality(int16)
                            )') AS json FROM test_low_cardinality_int;
SELECT json_extract(data, 'tuple(
                            a low_cardinality(int32),
                            b low_cardinality(int32),
                            c low_cardinality(int32),
                            d low_cardinality(int32)
                            )') AS json FROM test_low_cardinality_int;
SELECT json_extract(data, 'tuple(
                            a low_cardinality(int64),
                            b low_cardinality(int64),
                            c low_cardinality(int64),
                            d low_cardinality(int64)
                            )') AS json FROM test_low_cardinality_int;
SELECT json_extract(data, 'tuple(
                            a low_cardinality(uuid),
                            b low_cardinality(uuid),
                            c low_cardinality(uuid),
                            d low_cardinality(uuid)
                            )') AS json FROM test_low_cardinality_uuid;
DROP STREAM test_low_cardinality_string;
DROP STREAM test_low_cardinality_uuid;
DROP STREAM test_low_cardinality_int;
