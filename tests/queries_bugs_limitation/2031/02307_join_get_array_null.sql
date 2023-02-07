drop stream if exists id_val;

create stream id_val(id int32, val array(int32)) engine Join(ANY, LEFT, id) settings join_use_nulls = 1;
select join_get(id_val, 'val', to_int32(number)) from numbers(1);

drop stream id_val;
