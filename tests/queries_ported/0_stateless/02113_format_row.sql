set output_format_write_statistics=0;
select format_row('TSVWithNamesAndTypes', number, to_date(number)) from numbers(5);
select format_row('CSVWithNamesAndTypes', number, to_date(number)) from numbers(5);
select format_row('JSONCompactEachRowWithNamesAndTypes', number, to_date(number)) from numbers(5);
select format_row('XML', number, to_date(number)) from numbers(5);

