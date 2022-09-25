-- Tags: long

SET query_mode = 'table';
drop stream if exists stack;

set max_insert_threads = 4;

create stream stack(item_id int64, brand_id int64, rack_id int64, dt datetime, expiration_dt datetime, quantity uint64)
Engine = MergeTree 
partition by toYYYYMM(dt) 
order by (brand_id, to_start_of_hour(dt));

insert into stack 
select number%99991, number%11, number%1111, to_datetime('2020-01-01 00:00:00')+number/100, 
   to_datetime('2020-02-01 00:00:00')+number/10, int_div(number,100)+1
from numbers_mt(10000000);

select '---- arrays ----';

select cityHash64( to_string( groupArray (tuple(*) ) )) from (
    select brand_id, rack_id, array_join(arraySlice(arraySort(group_array(quantity)),1,2)) quantity
    from stack
    group by brand_id, rack_id
    order by brand_id, rack_id, quantity
) t;


select '---- window f ----';

select cityHash64( to_string( groupArray (tuple(*) ) )) from (
    select brand_id, rack_id,  quantity from
       ( select brand_id, rack_id, quantity, row_number() over (partition by brand_id, rack_id order by quantity) rn
         from stack ) as t0 
    where rn <= 2  
    order by brand_id, rack_id, quantity
) t;

drop stream if exists stack;
