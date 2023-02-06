drop stream if exists stream_02152;

create stream stream_02152 (a string, b low_cardinality(string)) engine = MergeTree order by a;
insert into stream_02152 values ('a_1', 'b_1') ('a_2', 'b_2') ('a_1', 'b_3') ('a_2', 'b_2');

set count_distinct_optimization=true;
select countDistinct(a) from stream_02152;
select countDistinct(b) from stream_02152;
select uniqExact(m) from (select number, (number / 2)::uint64 as m from numbers(10));
select uniqExact(x) from numbers(10) group by number % 2 as x;

set count_distinct_optimization=false;
select countDistinct(a) from stream_02152;
select countDistinct(b) from stream_02152;
select uniqExact(m) from (select number, (number / 2)::uint64 as m from numbers(10));
select uniqExact(x) from numbers(10) group by number % 2 as x;

drop stream if exists stream_02152;
