DROP STREAM IF EXISTS stream_with_enum_column_for_csv_insert;

CREATE STREAM stream_with_enum_column_for_csv_insert (
    Id int32,
    Value enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

SET input_format_csv_enum_as_number = 1;

INSERT INTO stream_with_enum_column_for_csv_insert FORMAT CSV 102,2
SELECT * FROM stream_with_enum_column_for_csv_insert;

SET input_format_csv_enum_as_number = 0;

DROP STREAM IF EXISTS stream_with_enum_column_for_csv_insert;
