-- Tags: no-fasttest

select format_row('ORC', number, to_date(number)) from numbers(5); -- { serverError 36 }
select format_row('Parquet', number, to_date(number)) from numbers(5); -- { serverError 36 }
select format_row('Arrow', number, to_date(number)) from numbers(5); -- { serverError 36 }
select format_row('Native', number, to_date(number)) from numbers(5); -- { serverError 36 }
