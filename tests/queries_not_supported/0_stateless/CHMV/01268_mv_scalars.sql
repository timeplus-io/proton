SET query_mode = 'table';
DROP STREAM IF EXISTS dest_table_mv;
DROP STREAM IF EXISTS left_table;
DROP STREAM IF EXISTS right_table;
DROP STREAM IF EXISTS dest_table;
DROP STREAM IF EXISTS src_table;
DROP VIEW IF EXISTS dst_mv;

create stream src_table Engine=Memory as system.numbers;
CREATE MATERIALIZED VIEW dst_mv Engine=Memory as select *, (SELECT count() FROM src_table) AS cnt FROM src_table;
insert into src_table select * from numbers(2);
insert into src_table select * from numbers(2);
insert into src_table select * from numbers(2);
select * from dst_mv order by number;

create stream dest_table (`date` date, `Id` uint64, `Units` Float32) ;
create stream left_table as dest_table;
create stream right_table as dest_table;
insert into right_table select to_date('2020-01-01') + number, number, number / 2 from numbers(10);

CREATE MATERIALIZED VIEW dest_table_mv TO dest_table as select * FROM (SELECT * FROM left_table) AS t1 INNER JOIN (WITH (SELECT DISTINCT date FROM left_table LIMIT 1) AS dt SELECT * FROM right_table WHERE date = dt) AS t2 USING (date, Id);

insert into left_table select to_date('2020-01-01'), 0, number * 2 from numbers(3);
select 'the rows get inserted';
select * from dest_table order by date, Id, Units;

insert into left_table select to_date('2020-01-01'), 5, number * 2 from numbers(3);
select 'no new rows';
select * from dest_table order by date, Id, Units;

truncate table left_table;
insert into left_table select to_date('2020-01-01') + 5, 5, number * 2 from numbers(3);
select 'the rows get inserted';
select * from dest_table order by date, Id, Units;

drop stream dest_table_mv;
drop stream left_table;
drop stream right_table;
drop stream dest_table;
drop stream src_table;
drop view dst_mv;
