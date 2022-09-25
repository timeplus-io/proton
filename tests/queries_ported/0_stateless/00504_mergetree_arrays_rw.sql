SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

drop stream if exists test_ins_arr;
create stream test_ins_arr (date date, val array(uint64)) engine = MergeTree(date, (date), 8192);
insert into test_ins_arr(date, val) select to_date('2017-10-02'), [number, 42] from system.numbers limit 10000;
select sleep(3);
select * from test_ins_arr limit 10;
drop stream test_ins_arr;

drop stream if exists test_ins_null;
create stream test_ins_null (date date, val nullable(uint64)) engine = MergeTree(date, (date), 8192);
insert into test_ins_null(date, val) select to_date('2017-10-02'), if(number % 2 <>0, number, Null) from system.numbers limit 10000;
select sleep(3);
select * from test_ins_null limit 10;
drop stream test_ins_null;

drop stream if exists test_ins_arr_null;
create stream test_ins_arr_null (date date, val array(nullable(uint64))) engine = MergeTree(date, (date), 8192);
insert into test_ins_arr_null(date, val) select to_date('2017-10-02'), [if(number % 2 <>0, number, Null), number, Null] from system.numbers limit 10000;
select sleep(3);
select * from test_ins_arr_null limit 10;
drop stream test_ins_arr_null;

drop stream if exists test_ins_arr_arr;
create stream test_ins_arr_arr (date date, val array(array(uint64))) engine = MergeTree(date, (date), 8192);
insert into test_ins_arr_arr(date,val) select to_date('2017-10-02'), [[number],[number + 1, number + 2]] from system.numbers limit 10000;
select sleep(3);
select * from test_ins_arr_arr limit 10;
drop stream test_ins_arr_arr;

drop stream if exists test_ins_arr_arr_null;
create stream test_ins_arr_arr_null (date date, val array(array(nullable(uint64)))) engine = MergeTree(date, (date), 8192);
insert into test_ins_arr_arr_null(date,val) select to_date('2017-10-02'), [[1, Null, number], [3, Null, number]] from system.numbers limit 10000;
select sleep(3);
select * from test_ins_arr_arr_null limit 10;
drop stream test_ins_arr_arr_null;

drop stream if exists test_ins_arr_arr_arr;
create stream test_ins_arr_arr_arr (date date, val array(array(array(uint64)))) engine = MergeTree(date, (date), 8192);
insert into test_ins_arr_arr_arr(date,val) select to_date('2017-10-02'), [[[number]],[[number + 1], [number + 2, number + 3]]] from system.numbers limit 10000;
select sleep(3);
select * from test_ins_arr_arr_arr limit 10;
drop stream test_ins_arr_arr_arr;
