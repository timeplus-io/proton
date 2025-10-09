-- Tags: no-fasttest

select '{"id":1,"foo":["bar"]}' as a, json_merge_patch(a,to_JSON_string(map_cast('foo',array_push_back(array_map(x->JSON_extract_string(x),JSON_extract_array_raw(a, 'foo')),'baz')))) as b;
