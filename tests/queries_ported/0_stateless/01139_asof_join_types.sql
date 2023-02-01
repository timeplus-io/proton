select * from (select 0 as k, to_int8(1) as v) t1 asof join (select 0 as k, to_int8(0) as v) t2 using(k, v);
select * from (select 0 as k, to_int16(1) as v) t1 asof join (select 0 as k, to_int16(0) as v) t2 using(k, v);
select * from (select 0 as k, to_int32(1) as v) t1 asof join (select 0 as k, to_int32(0) as v) t2 using(k, v);
select * from (select 0 as k, to_int64(1) as v) t1 asof join (select 0 as k, to_int64(0) as v) t2 using(k, v);

select * from (select 0 as k, to_uint8(1) as v) t1 asof join (select 0 as k, to_uint8(0) as v) t2 using(k, v);
select * from (select 0 as k, to_uint16(1) as v) t1 asof join (select 0 as k, to_uint16(0) as v) t2 using(k, v);
select * from (select 0 as k, to_uint32(1) as v) t1 asof join (select 0 as k, to_uint32(0) as v) t2 using(k, v);
select * from (select 0 as k, to_uint64(1) as v) t1 asof join (select 0 as k, to_uint64(0) as v) t2 using(k, v);

select * from (select 0 as k, to_decimal32(1, 0) as v) t1 asof join (select 0 as k, to_decimal32(0, 0) as v) t2 using(k, v);
select * from (select 0 as k, to_decimal64(1, 0) as v) t1 asof join (select 0 as k, to_decimal64(0, 0) as v) t2 using(k, v);
select * from (select 0 as k, to_decimal128(1, 0) as v) t1 asof join (select 0 as k, to_decimal128(0, 0) as v) t2 using(k, v);

select * from (select 0 as k, to_date(0) as v) t1 asof join (select 0 as k, to_date(0) as v) t2 using(k, v);
select * from (select 0 as k, to_datetime(0, 'UTC') as v) t1 asof join (select 0 as k, to_datetime(0, 'UTC') as v) t2 using(k, v);

select * from (select 0 as k, 'x' as v) t1 asof join (select 0 as k, 'x' as v) t2 using(k, v); -- { serverError 169 }
