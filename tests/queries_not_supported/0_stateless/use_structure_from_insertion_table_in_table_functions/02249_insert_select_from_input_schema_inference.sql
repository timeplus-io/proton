set use_structure_from_insertion_table_in_table_functions = 1;

drop stream if exists test_02249;
create stream test_02249 (x uint32, y string) engine=Memory();
insert into test_02249 select * from input() format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};
select * from test_02249;
drop stream test_02249;
