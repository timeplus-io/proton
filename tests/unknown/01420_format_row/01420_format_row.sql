select format_row('CSV', number, 'good') from numbers(3);
select format_row_no_newline('TSV', number, DATE '2001-12-12', 1.4) from numbers(3);
select format_row('JSONEachRow', number, to_nullable(3), Null) from numbers(3);
select format_row_no_newline('JSONEachRow', *) from numbers(3);

-- unknown format
select format_row('aaa', *) from numbers(3); -- { serverError 73 }
