-- Tags: not_supported, blocked_by_SummingMergeTree
SET query_mode = 'table';
drop stream if exists nested_map_multiple_values;

create stream nested_map_multiple_values (d materialized today(), k uint64, payload materialized rand(), SomeMap nested(ID uint32, Num1 Int64, Num2 float64)) engine=SummingMergeTree(d, k, 8192);

insert into nested_map_multiple_values values (0,[1],[100],[1.0]),(1,[1],[100],[1.0]),(2,[1],[100],[1.0]),(3,[1,2],[100,150],[1.0,1.5]);
insert into nested_map_multiple_values values (0,[2],[150],[-2.5]),(1,[1],[150],[-1.0]),(2,[1,2],[150,150],[2.5,3.5]),(3,[1],[-100],[-1]);
optimize table nested_map_multiple_values;
select * from nested_map_multiple_values;

drop stream nested_map_multiple_values;

drop stream if exists nested_not_a_map;
create stream nested_not_a_map (d materialized today(), k uint64, payload materialized rand(), OnlyOneColumnMap nested(ID uint32), NonArithmeticValueMap nested(ID uint32, date date), Nested_ nested(ID uint32, Num Int64)) engine=SummingMergeTree(d, k, 8192);

insert into nested_not_a_map values (0,[1],[1],['2015-04-09'],[1],[100]);
insert into nested_not_a_map values (0,[1],[1],['2015-04-08'],[1],[200]);
optimize table nested_not_a_map;
select * from nested_not_a_map;

drop stream nested_not_a_map;
