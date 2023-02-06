set use_structure_from_insertion_table_in_table_functions = 1;

insert into stream function file('data_02250.jsonl') select NULL as x settings engine_file_truncate_on_insert=1;
drop stream if exists test_02250;
create stream test_02250 (x nullable(uint32)) engine=Memory();
insert into test_02250 select * from file('data_02250.jsonl');
select * from test_02250;
drop stream test_02250;
