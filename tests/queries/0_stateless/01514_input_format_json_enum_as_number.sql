-- Tags: no-fasttest

DROP STREAM IF EXISTS table_with_enum_column_for_json_insert;

create stream table_with_enum_column_for_json_insert (
    Id int32,
    Value Enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

INSERT INTO table_with_enum_column_for_json_insert FORMAT JSONEachRow {"Id":102,"Value":2}
SELECT * FROM table_with_enum_column_for_json_insert;

DROP STREAM IF EXISTS table_with_enum_column_for_json_insert;
