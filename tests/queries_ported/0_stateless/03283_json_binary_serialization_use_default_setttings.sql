set enable_json_type=1;
set output_format_binary_write_json_as_string=1;

drop stream if exists test;
create stream test (json json(max_dynamic_paths=0)) engine=Memory;
insert into test format JSONAsObject {"a" : [{"b" : 42}]};

select * from test;
