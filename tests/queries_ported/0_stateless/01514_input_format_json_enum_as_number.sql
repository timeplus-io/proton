-- Tags: no-fasttest

DROP STREAM IF EXISTS stream_with_enum_column_for_json_insert;

CREATE STREAM stream_with_enum_column_for_json_insert (
    Id int32,
    Value enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

INSERT INTO stream_with_enum_column_for_json_insert FORMAT JSONEachRow {"Id":102,"Value":2}
SELECT * FROM stream_with_enum_column_for_json_insert;

DROP STREAM IF EXISTS stream_with_enum_column_for_json_insert;
