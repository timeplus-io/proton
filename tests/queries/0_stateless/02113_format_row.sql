select formatRow('TSVWithNamesAndTypes', number, to_date(number)) from numbers(5);
select formatRow('CSVWithNamesAndTypes', number, to_date(number)) from numbers(5);
select formatRow('JSONCompactEachRowWithNamesAndTypes', number, to_date(number)) from numbers(5);
select formatRow('XML', number, to_date(number)) from numbers(5);

