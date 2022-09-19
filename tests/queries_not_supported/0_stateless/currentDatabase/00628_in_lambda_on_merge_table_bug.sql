SET query_mode = 'table';
drop stream if exists test_in_tuple_1;
drop stream if exists test_in_tuple_2;
drop stream if exists test_in_tuple;

create stream test_in_tuple_1 (key int32, key_2 int32, x array(int32), y array(int32)) engine = MergeTree order by (key, key_2);
create stream test_in_tuple_2 (key int32, key_2 int32, x array(int32), y array(int32)) engine = MergeTree order by (key, key_2);
create stream test_in_tuple as test_in_tuple_1 engine = Merge(currentDatabase(), '^test_in_tuple_[0-9]+$');

insert into test_in_tuple_1 values (1, 1, [1, 2], [1, 2]);
insert into test_in_tuple_2 values (2, 1, [1, 2], [1, 2]);
select key, arr_x, arr_y, _table from test_in_tuple left array join x as arr_x, y as arr_y order by _table;
select '-';
select key, arr_x, arr_y, _table from test_in_tuple left array join x as arr_x, y as arr_y where (key_2, arr_x, arr_y) in (1, 1, 1) order by _table;
select '-';
select key, arr_x, arr_y, _table from test_in_tuple left array join array_filter((t, x_0, x_1) -> (key_2, x_0, x_1) in (1, 1, 1), x, x ,y) as arr_x, array_filter((t, x_0, x_1) -> (key_2, x_0, x_1) in (1, 1, 1), y, x ,y) as arr_y where (key_2, arr_x, arr_y) in (1, 1, 1) order by _table;

drop stream if exists test_in_tuple_1;
drop stream if exists test_in_tuple_2;
drop stream if exists test_in_tuple;
