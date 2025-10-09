SET query_mode='table';
set asterisk_include_reserved_columns=false;

-- { echoOn }
set use_with_fill_by_sorting_prefix=1;

-- corner case with constant sort prefix
SELECT number
FROM numbers(1)
ORDER BY 10 ASC, number DESC WITH FILL FROM 1
SETTINGS enable_positional_arguments=0;

-- sensor stream
drop stream if exists ts;
create stream ts (sensor_id uint64, timestamp uint64, value float64)  ORDER BY (sensor_id, timestamp);
select sleep(2) FORMAT Null;
insert into ts(sensor_id, timestamp, value) VALUES (1, 10, 1), (1, 12, 2), (3, 5, 1), (3, 7, 3), (5, 1, 1), (5, 3, 1);
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;
-- FillingTransform: 6 rows will be processed in 1 chunks
select * from ts order by sensor_id, timestamp with fill step 1;

drop stream if exists ts;
create stream ts (sensor_id uint64, timestamp uint64, value float64)  ORDER BY (sensor_id, timestamp);
select sleep(2) FORMAT Null;
system stop merges ts;
-- FillingTransform: 6 rows will be processed in 3 chunks with 2 rows each
insert into ts(sensor_id, timestamp, value) VALUES (1, 10, 1), (1, 12, 1);
insert into ts(sensor_id, timestamp, value) VALUES (3, 5, 1), (3, 7, 1);
insert into ts(sensor_id, timestamp, value) VALUES (5, 1, 1), (5, 3, 1);
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;
select * from ts order by sensor_id, timestamp with fill step 1 settings max_block_size=2;

drop stream if exists ts;
create stream ts (sensor_id uint64, timestamp uint64, value float64)  ORDER BY (sensor_id, timestamp);
select sleep(2) FORMAT Null;
system stop merges ts;
-- FillingTransform: 6 rows will be processed in 2 chunks with 3 rows each
insert into ts(sensor_id, timestamp, value) VALUES (1, 10, 1), (1, 12, 1), (3, 5, 1);
insert into ts(sensor_id, timestamp, value) VALUES (3, 7, 1), (5, 1, 1), (5, 3, 1);
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;
select * from ts order by sensor_id, timestamp with fill step 1 settings max_block_size=3;

-- FROM and TO
-- ASC order in sorting prefix
select * from ts order by sensor_id, timestamp with fill from 6 to 10 step 1 interpolate (value as 9999);
select * from ts order by sensor_id, timestamp with fill from 6 to 10 step 1 interpolate (value as 9999) settings use_with_fill_by_sorting_prefix=0;

-- DESC order in sorting prefix
select * from ts order by sensor_id DESC, timestamp with fill from 6 to 10 step 1 interpolate (value as 9999);
select * from ts order by sensor_id DESC, timestamp with fill from 6 to 10 step 1 interpolate (value as 9999) settings use_with_fill_by_sorting_prefix=0;

-- without TO
-- ASC order in sorting prefix
select * from ts order by sensor_id, timestamp with fill from 6 step 1 interpolate (value as 9999);
select * from ts order by sensor_id, timestamp with fill from 6 step 1 interpolate (value as 9999) settings use_with_fill_by_sorting_prefix=0;
-- DESC order in sorting prefix
select * from ts order by sensor_id DESC, timestamp with fill from 6 step 1 interpolate (value as 9999);
select * from ts order by sensor_id DESC, timestamp with fill from 6 step 1 interpolate (value as 9999) settings use_with_fill_by_sorting_prefix=0;

-- without FROM
-- ASC order in sorting prefix
select * from ts order by sensor_id, timestamp with fill to 10 step 1 interpolate (value as 9999);
select * from ts order by sensor_id, timestamp with fill to 10 step 1 interpolate (value as 9999) settings use_with_fill_by_sorting_prefix=0;
-- DESC order in sorting prefix
select * from ts order by sensor_id DESC, timestamp with fill to 10 step 1 interpolate (value as 9999);
select * from ts order by sensor_id DESC, timestamp with fill to 10 step 1 interpolate (value as 9999) settings use_with_fill_by_sorting_prefix=0;

-- checking that sorting prefix columns can't be used in INTERPOLATE
SELECT * FROM ts ORDER BY sensor_id, value, timestamp WITH FILL FROM 6 TO 10 INTERPOLATE ( value AS 1 ); -- { serverError INVALID_WITH_FILL_EXPRESSION }
