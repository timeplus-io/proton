-- Tags: no-fasttest

set output_format_json_array_of_rows = 1;
select number as a, number * 2 as b from numbers(3) format JSONEachRow;
select * from numbers(1) format JSONEachRow;
select * from numbers(1) where null format JSONEachRow;
