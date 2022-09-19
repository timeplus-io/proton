-- Tags: no-fasttest

select formatRow('ORC', number, to_date(number)) from numbers(5); -- { serverError 36 }
select formatRow('Parquet', number, to_date(number)) from numbers(5); -- { serverError 36 }
select formatRow('Arrow', number, to_date(number)) from numbers(5); -- { serverError 36 }
select formatRow('Native', number, to_date(number)) from numbers(5); -- { serverError 36 }
