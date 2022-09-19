SET query_mode = 'table';
drop stream if exists test1_00863;
drop stream if exists test2_00863;
drop stream if exists test3_00863;

create stream test1_00863 (id uint64, code string) engine = Memory;
create stream test3_00863 (id uint64, code string) engine = Memory;
create stream test2_00863 (id uint64, code string, test1_id uint64, test3_id uint64) engine = Memory;

insert into test1_00863 (id, code) select number, to_string(number) FROM numbers(100000);
insert into test3_00863 (id, code) select number, to_string(number) FROM numbers(100000);
insert into test2_00863 (id, code, test1_id, test3_id) select number, to_string(number), number, number FROM numbers(100000);

SET max_memory_usage = 50000000;

select test2_00863.id
from test1_00863, test2_00863, test3_00863
where test1_00863.code in ('1', '2', '3')
    and test2_00863.test1_id = test1_00863.id
    and test2_00863.test3_id = test3_00863.id;

drop stream test1_00863;
drop stream test2_00863;
drop stream test3_00863;
