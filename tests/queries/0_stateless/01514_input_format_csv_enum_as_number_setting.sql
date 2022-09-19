DROP STREAM IF EXISTS table_with_enum_column_for_csv_insert;

create stream table_with_enum_column_for_csv_insert (
    Id int32,
    Value Enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

SET input_format_csv_enum_as_number = 1;

INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2
SELECT * FROM table_with_enum_column_for_csv_insert;

SET input_format_csv_enum_as_number = 0;

DROP STREAM IF EXISTS table_with_enum_column_for_csv_insert;
