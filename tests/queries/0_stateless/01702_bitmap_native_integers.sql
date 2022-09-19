SET query_mode = 'table';
drop stream if exists t;
create stream t(i8 int8, i16 Int16, i32 int32, i64 int64) engine Memory;
insert into t values (-1, -1, -1, -1), (-2, -2, -2, -2), (-3, -3, -3, -3), (-4, -4, -4, -4), (-5, -5, -5, -5);
select * apply bitmapMin, * apply bitmapMax from (select * apply groupBitmapState from t);
drop stream t;
