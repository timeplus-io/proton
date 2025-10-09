drop view if exists tail_created_before_delete;
select sleep(1) format Null;
drop view if exists seek_to_earliest_created_before_delete;
select sleep(1) format Null;
drop view if exists mv_seek_to_earliest_created_after_delete_backfill;
select sleep(1) format Null;
drop view if exists mv_seek_to_earliest_created_after_delete_no_backfill;
select sleep(1) format Null;

DROP STREAM IF EXISTS 99037_sales;
select sleep(1) format Null;
SET allow_experimental_lightweight_delete = 1;
SET mutations_sync = 2;

CREATE STREAM 99037_sales (
    id uint32,
    product string,
    amount decimal(10, 2)
);

select sleep(1) format Null;

create materialized view tail_created_before_delete as select * from 99037_sales;

create materialized view seek_to_earliest_created_before_delete as select * from 99037_sales settings seek_to='earliest';

select sleep(1) format Null;

INSERT INTO 99037_sales(id, product, amount)
VALUES (1, 'Phone', 599.99), (2, 'Laptop', 1299.99), (3, 'Tablet', 399.99);

select sleep(2) format Null;

DELETE FROM 99037_sales WHERE id = 2;

select sleep(3) format Null;
select sleep(3) format Null;

create materialized view mv_seek_to_earliest_created_after_delete_no_backfill as SELECT id, product, amount FROM 99037_sales settings seek_to='earliest', enable_backfill_from_historical_store = false;
create materialized view mv_seek_to_earliest_created_after_delete_backfill as SELECT id, product, amount FROM 99037_sales settings seek_to='earliest', enable_backfill_from_historical_store = true;
select sleep(3) format Null;
select sleep(3) format Null;
select sleep(3) format Null;
select sleep(3) format Null;


select 'after delete, the historical data will filter out the deleted row during select';
SELECT id, product, amount FROM table(99037_sales) ORDER BY id;



select 'created after delete, the new tail query(select * from 99037_sales settings seek_to=earliest, enable_backfill_from_historical_store = false) will print the normal streaming store data';
select id, product, amount from table(mv_seek_to_earliest_created_after_delete_no_backfill) ORDER BY id;

select 'created after delete, the new backfill query(select * from 99037_sales settings seek_to=earliest, enable_backfill_from_historical_store = true) will concat the filtered historical data and the original streaming data (dup data)';
select id, product, amount from table(mv_seek_to_earliest_created_after_delete_backfill) ORDER BY id;






select 'created before delete, the OLD tail query will not emit new data';
select id, product, amount from table(tail_created_before_delete) ORDER BY id;

select 'created before delete, the OLD seek_to_earliest query will not emit new data';
select id, product, amount from table(seek_to_earliest_created_before_delete) ORDER BY id;


drop view if exists tail_created_before_delete;
select sleep(1) format Null;
drop view if exists seek_to_earliest_created_before_delete;
select sleep(1) format Null;
drop view if exists mv_seek_to_earliest_created_after_delete_backfill;
select sleep(1) format Null;
drop view if exists mv_seek_to_earliest_created_after_delete_no_backfill;
select sleep(1) format Null;

DROP STREAM IF EXISTS 99037_sales;
