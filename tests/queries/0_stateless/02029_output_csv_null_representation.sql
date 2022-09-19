DROP STREAM IF EXISTS test_data;
create stream test_data (
    col1 Nullable(string),
    col2 Nullable(string),
    col3 Nullable(string)
) ;

INSERT INTO test_data VALUES ('val1', NULL, 'val3');

SELECT '# format_csv_null_representation should initially be \\N';
SELECT * FROM test_data FORMAT CSV;

SELECT '# Changing format_csv_null_representation';
SET format_csv_null_representation = '∅';
SELECT * FROM test_data FORMAT CSV;
SET format_csv_null_representation = '\\N';
