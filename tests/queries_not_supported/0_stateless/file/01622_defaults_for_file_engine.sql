insert into stream function file("data1622.json", "TSV", "value string") VALUES ('{"a":1}');
drop stream if exists json;
create stream json(a int, b int default 7, c default a + b) engine File(JSONEachRow, 'data1622.json');
set input_format_defaults_for_omitted_fields = 1;
select * from json;
truncate stream json;
drop stream if exists json;
