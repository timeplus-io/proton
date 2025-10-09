-- Tags: no-fasttest
select json_merge_patch(null);
select json_merge_patch('{"a":1}');
select json_merge_patch('{"a":1}', '{"b":1}');
select json_merge_patch('{"a":1}', '{"b":1}', '{"c":[1,2]}');
select json_merge_patch('{"a":1}', '{"b":1}', '{"c":[{"d":1},2]}');
select json_merge_patch('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');
select json_merge_patch('{"a": "1","b": 2,"c": [true,{"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}');
select json_merge_patch('{"a": {"b": 1, "c": 2}}', '{"a": {"b": [3, 4]}}');
select json_merge_patch('{ "a": 1, "b":2 }','{ "a": 3, "c":4 }','{ "a": 5, "d":6 }');
select json_merge_patch('{"a":1, "b":2}', '{"b":null}');

select json_merge_patch('[1]'); -- { serverError BAD_ARGUMENTS }
select json_merge_patch('{"a": "1","b": 2,"c": [true,"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}'); -- { serverError BAD_ARGUMENTS }

drop stream if exists default.t_json_merge;
create stream default.t_json_merge (id uint64, s1 string, s2 string) engine = Memory;

insert into default.t_json_merge select number, format('{{ "k{0}": {0} }}', to_string(number * 2)), format('{{ "k{0}": {0} }}', to_string(number * 2 + 1)) from numbers(5);
insert into default.t_json_merge select number, format('{{ "k{0}": {0} }}', to_string(number * 2)), format('{{ "k{0}": {0}, "k{1}": 222 }}', to_string(number * 2 + 1), to_string(number * 2)) from numbers(5, 5);

select json_merge_patch(s1, s2) from default.t_json_merge ORDER BY id;

drop stream default.t_json_merge;
