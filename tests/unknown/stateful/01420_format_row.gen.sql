-- Tags: no-fasttest

set output_format_write_statistics=0;

select 'CSV';
select 'format_row';
select format_row('CSV', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('CSV', number, good) from (select number, 'good' as good from numbers(3));

select 'TSV';
select 'format_row';
select format_row('TSV', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('TSV', number, good) from (select number, 'good' as good from numbers(3));

select 'JSONEachRow';
select 'format_row';
select format_row('JSONEachRow', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('JSONEachRow', number, good) from (select number, 'good' as good from numbers(3));

select 'JSONCompactEachRow';
select 'format_row';
select format_row('JSONCompactEachRow', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('JSONCompactEachRow', number, good) from (select number, 'good' as good from numbers(3));

select 'TSKV';
select 'format_row';
select format_row('TSKV', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('TSKV', number, good) from (select number, 'good' as good from numbers(3));

select 'XML';
select 'format_row';
select format_row('XML', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('XML', number, good) from (select number, 'good' as good from numbers(3));

select 'Markdown';
select 'format_row';
select format_row('Markdown', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('Markdown', number, good) from (select number, 'good' as good from numbers(3));

select 'CustomSeparated';
select 'format_row';
select format_row('CustomSeparated', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('CustomSeparated', number, good) from (select number, 'good' as good from numbers(3));

select 'SQLInsert';
select 'format_row';
select format_row('SQLInsert', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('SQLInsert', number, good) from (select number, 'good' as good from numbers(3));

select 'Vertical';
select 'format_row';
select format_row('Vertical', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('Vertical', number, good) from (select number, 'good' as good from numbers(3));

select 'JSON';
select 'format_row';
select format_row('JSON', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('JSON', number, good) from (select number, 'good' as good from numbers(3));

select 'JSONCompact';
select 'format_row';
select format_row('JSONCompact', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('JSONCompact', number, good) from (select number, 'good' as good from numbers(3));

select 'Values';
select 'format_row';
select format_row('Values', number, good) from (select number, 'good' as good from numbers(3));
select 'format_row_no_newline';
select format_row_no_newline('Values', number, good) from (select number, 'good' as good from numbers(3));

-- unknown format
select format_row('aaa', *) from numbers(3); -- { serverError 73 }

