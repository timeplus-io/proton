-- Tags: no-parallel

INSERT INTO FUNCTION file('data_03198_table_function_directory_path.csv', 'CSV') SELECT '1.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/1.csv', 'CSV') SELECT '1.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/2.csv', 'CSV') SELECT '2.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/dir/3.csv', 'CSV') SELECT '3.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/dir1/dir/4.csv', 'CSV') SELECT '4.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/dir2/dir/5.csv', 'CSV') SELECT '5.csv' SETTINGS engine_file_truncate_on_insert=1;

SELECT COUNT(*) FROM file('data_03198_table_function_directory_path', 'CSV'); 
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path/', 'CSV');
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path/dir', 'CSV');
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path/*/dir', 'CSV'); -- { serverError CANNOT_READ_FROM_FILE_DESCRIPTOR }
SELECT COUNT(*) FROM file('data_03198_table_function_directory_pat'); -- { serverError BAD_ARGUMENTS }
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path.csv', 'CSV');
