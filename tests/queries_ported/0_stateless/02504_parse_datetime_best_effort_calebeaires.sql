CREATE TEMPORARY STREAM my_table (col_date Date, col_date32 Date32, col_datetime DateTime('UTC'), col_datetime32 DateTime32('UTC'), col_datetime64 DateTime64) ENGINE = Memory();
insert into `my_table` (`col_date`, `col_date32`, `col_datetime`, `col_datetime32`, `col_datetime64`) values (parse_datetime64_best_effort('1969-01-01'), '1969-01-01', parse_datetime64_best_effort('1969-01-01 10:42:00'), parse_datetime64_best_effort('1969-01-01 10:42:00'), parse_datetime64_best_effort('1969-01-01 10:42:00'));

-- The values for Date32 and DateTime64 will be year 1969, while the values of Date, DateTime will contain a value affected by implementation-defined overflow and can be arbitrary.
SELECT to_year(col_date), col_date32, to_year(col_datetime), to_year(col_datetime32), col_datetime64 FROM my_table;
